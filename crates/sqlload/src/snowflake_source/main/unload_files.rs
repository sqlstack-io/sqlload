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
    let UnloadFolder::External {
        folder: location, ..
    } = &folder
    else {
        return Err(SnowflakeSourceError::Invalid(
            "only files unloaded to S3 are read by the destination itself".to_owned(),
        )
        .into());
    };
    let (target, copy) = copy_statement(&folder, &query);
    let loaded = async {
        let copied = execute_sql(&reader.sql, &copy)
            .await
            .map_err(SnowflakeSourceError::from)?;
        let rows = rows_unloaded(&copied)?;
        let keys = match rows {
            0 => Vec::new(),
            _ => match unloaded_files(reader, &folder).await? {
                Unloaded::External { files, .. } => files,
                Unloaded::Internal { stage_path, .. } => {
                    return Err(SnowflakeSourceError::Invalid(format!(
                        "{stage_path} is an internal stage, which the destination cannot read"
                    ))
                    .into());
                }
            },
        };
        let files = UnloadedFiles {
            folder: location.clone(),
            keys,
            rows,
        };
        load(&files).await
    }
    .await;
    if let Err(error) = remove_folder(reader, &folder).await {
        tracing::warn!(folder = %target, %error, "cannot remove the unloaded files");
    }
    loaded
}
