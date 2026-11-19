//! Export a table or query as Parquet with `EXPORT DATA`, for the destination to read itself.

use serde_json::json;

use crate::bigquery_api::main::load_job::load_job;
use crate::bigquery_source::_helpers::export::{export_columns, export_statement};
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::main::selected_rows_query::selected_rows_query;
use crate::bigquery_source::models::{BigQueryReadRequest, BigQueryReader};
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::snowflake_source::models::UnloadedFiles;
use crate::store_paths::models::Location;

/// Export `request` with one `EXPORT DATA` query job into a folder of its own below the reader's export folder and pass its files to `load`, which has the destination read them. The rows `load` must find are the ones the job reports exporting. The folder is removed afterwards, after a failure too (a folder that cannot be removed is only a warning). An export of no rows passes no files. The rows are the ones [`selected_rows_query`] selects, as of the same instant, with times written as whole microseconds.
pub async fn export_files<T, E: From<BigQuerySourceError>>(
    reader: &BigQueryReader,
    request: &BigQueryReadRequest<'_>,
    load: impl AsyncFnOnce(&UnloadedFiles) -> Result<T, E>,
) -> Result<T, E> {
    let invalid = |text: String| E::from(BigQuerySourceError::Invalid(text));
    let Some(base) = &reader.export else {
        return Err(invalid(
            "the source sets no export folder for BigQuery to write to".to_owned(),
        ));
    };
    let columns = export_columns(request).map_err(invalid)?;
    let select = selected_rows_query(
        reader,
        &BigQueryReadRequest {
            columns: &columns,
            ..*request
        },
    );
    let folder = Location {
        store: base.store.clone(),
        prefix: format!(
            "{}sqlload_export_{}/",
            base.prefix,
            uuid::Uuid::new_v4().simple()
        ),
    };
    let loaded = async {
        let rows = exported_rows(reader, &export_statement(&folder, &select)).await?;
        let keys = match rows {
            0 => Vec::new(),
            _ => folder_keys(&folder).await?,
        };
        load(&UnloadedFiles {
            folder: folder.clone(),
            keys,
            rows,
        })
        .await
    }
    .await;
    if let Err(error) = remove_folder(&folder).await {
        tracing::warn!(folder = %folder, %error, "cannot remove the exported files");
    }
    loaded
}

/// Run the `EXPORT DATA` statement as one query job: the rows it exported, from its export statistics.
async fn exported_rows(
    reader: &BigQueryReader,
    statement: &str,
) -> Result<u64, BigQuerySourceError> {
    let client = &reader.client;
    let job = json!({
        "jobReference": {
            "projectId": client.project,
            "jobId": format!("sqlload_export_{}", uuid::Uuid::new_v4().simple()),
            "location": client.location,
        },
        "configuration": {"query": {"query": statement, "useLegacySql": false}},
    });
    let resource = load_job(client, &job, None).await?;
    let error = &resource["status"]["errorResult"];
    if !error.is_null() {
        return Err(BigQuerySourceError::Invalid(format!(
            "BigQuery could not export the rows: {}: {}",
            error["reason"].as_str().unwrap_or_default(),
            error["message"].as_str().unwrap_or_default()
        )));
    }
    let count = &resource["statistics"]["query"]["exportDataStatistics"]["rowCount"];
    count
        .as_str()
        .and_then(|text| text.parse().ok())
        .or_else(|| count.as_u64())
        .ok_or_else(|| {
            BigQuerySourceError::Invalid(format!(
                "BigQuery's export job reported {count} exported rows, not a count; the batch is not loaded"
            ))
        })
}

/// The keys of the files in `folder`, in order.
async fn folder_keys(folder: &Location) -> Result<Vec<String>, BigQuerySourceError> {
    let store = |error: crate::file_store::errors::StoreError| {
        BigQuerySourceError::Invalid(format!("the exported files in {folder}: {error}"))
    };
    let opened = open_folder(folder).map_err(store)?;
    let mut keys: Vec<String> = list_files(&opened)
        .await
        .map_err(store)?
        .into_iter()
        .map(|file| file.relative_key)
        .collect();
    keys.sort();
    Ok(keys)
}

/// Remove `folder` and every file in it.
async fn remove_folder(folder: &Location) -> Result<(), crate::file_store::errors::StoreError> {
    let opened = open_folder(folder)?;
    let keys: Vec<String> = list_files(&opened)
        .await?
        .into_iter()
        .map(|file| file.relative_key)
        .collect();
    delete_files(&opened, &keys).await
}
