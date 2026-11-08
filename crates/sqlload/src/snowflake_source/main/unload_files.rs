//! Unload a table or query to S3 as Parquet for the destination to read itself.

use crate::snowflake_source::_helpers::folders::{
    Unloaded, remove_folder, unload_folder, unloaded_files,
};
use crate::snowflake_source::_helpers::unload::{copy_statement, rows_unloaded};
use crate::snowflake_source::_helpers::unload_query::unload_query;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::{
    SnowflakeReader, UnloadFolder, UnloadRequest, UnloadedFiles,
};
use crate::sql_api::main::execute_sql::execute_sql;

/// Unload `request` with `COPY INTO` into an S3 folder of its own and pass its files to `load`, which has the destination read them; the folder is removed afterwards, after a failure too (a folder that cannot be removed is only a warning). An unload of no rows passes no files.
pub async fn unload_files<T, E: From<SnowflakeSourceError>>(
    reader: &SnowflakeReader,
    request: &UnloadRequest<'_>,
    load: impl AsyncFnOnce(&UnloadedFiles) -> Result<T, E>,
) -> Result<T, E> {
    let query = unload_query(request).map_err(SnowflakeSourceError::Invalid)?;
    let run = uuid::Uuid::new_v4().simple().to_string();
    let folder = unload_folder(reader, &run).await?;
    let temp = tempfile::tempdir().map_err(|e| SnowflakeSourceError::Invalid(e.to_string()))?;
    let local = crate::store_paths::models::Location {
        store: crate::store_paths::models::StoreKind::Filesystem { root: temp.path().to_string_lossy().into_owned() },
        prefix: String::new(),
    };
    let location = match &folder {
        UnloadFolder::External { folder: location, .. } => location.clone(),
        UnloadFolder::Internal { .. } => local.clone(),
    };
    let (target, copy) = copy_statement(&folder, &query);
    let loaded = async {
        let t=std::time::Instant::now();
        let copied = execute_sql(&reader.sql, &copy)
            .await
            .map_err(SnowflakeSourceError::from)?;
        let rows = rows_unloaded(&copied)?;
        eprintln!("PROF copy {:?}", t.elapsed());
        let t=std::time::Instant::now();
        let keys = match rows {
            0 => Vec::new(),
            _ => match unloaded_files(reader, &folder).await? {
                Unloaded::External { files, .. } => files,
                internal @ Unloaded::Internal { .. } => {
                    use futures_util::StreamExt;
                    let td = std::time::Instant::now();
                    let names: Vec<String> = (0..internal.len()).map(|index| format!("part_{index:05}.parquet")).collect();
                    let mut fetched = futures_util::stream::iter(0..internal.len())
                        .map(|index| { let internal = &internal; async move { (index, internal.fetch(reader, index).await) } })
                        .buffer_unordered(std::env::var("SQLLOAD_PROBE_PD").ok().and_then(|v| v.parse().ok()).unwrap_or(crate::snowflake_source::constants::PARALLEL_DOWNLOADS));
                    while let Some((index, bytes)) = fetched.next().await {
                        let bytes = bytes?;
                        std::fs::write(temp.path().join(&names[index]), &bytes).map_err(|e| SnowflakeSourceError::Invalid(e.to_string()))?;
                    }
                    eprintln!("PROF download {:?}", td.elapsed());
                    names
                }
            },
        };
        let files = UnloadedFiles {
            folder: location.clone(),
            keys,
            rows,
        };
        eprintln!("PROF list {:?} files={}", t.elapsed(), files.keys.len());
        let t=std::time::Instant::now();
        let r = load(&files).await;
        eprintln!("PROF load {:?}", t.elapsed());
        r
    }
    .await;
    let t=std::time::Instant::now();
    let removed = remove_folder(reader, &folder).await;
    eprintln!("PROF remove {:?}", t.elapsed());
    if let Err(error) = removed {
        tracing::warn!(folder = %target, %error, "cannot remove the unloaded files");
    }
    loaded
}
