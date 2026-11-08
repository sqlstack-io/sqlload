//! Unload a table or query as Parquet for the destination to read itself.

use crate::snowflake_source::_helpers::folders::{
    Unloaded, destination_folder, remove_folder, unload_folder, unloaded_files,
};
use crate::snowflake_source::_helpers::unload::{copy_statement, rows_unloaded};
use crate::snowflake_source::_helpers::unload_query::unload_query;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::{SnowflakeReader, UnloadRequest, UnloadedFiles};
use crate::sql_api::main::execute_sql::execute_sql;

/// Unload `request` with `COPY INTO` into a folder of its own and pass its files to `load`, which has the destination read them: in place on S3, or from an internal stage downloaded unchanged into a local temporary folder (which needs room for the whole unload). Both folders are removed afterwards, after a failure too (a folder that cannot be removed is only a warning). An unload of no rows passes no files.
pub async fn unload_files<T, E: From<SnowflakeSourceError>>(
    reader: &SnowflakeReader,
    request: &UnloadRequest<'_>,
    load: impl AsyncFnOnce(&UnloadedFiles) -> Result<T, E>,
) -> Result<T, E> {
    let query = unload_query(request).map_err(SnowflakeSourceError::Invalid)?;
    let run = uuid::Uuid::new_v4().simple().to_string();
    let folder = unload_folder(reader, &run).await?;
    let (target, copy) = copy_statement(&folder, &query);
    let loaded = async {
        let copied = execute_sql(&reader.sql, &copy)
            .await
            .map_err(SnowflakeSourceError::from)?;
        let rows = rows_unloaded(&copied)?;
        let (location, downloads) = destination_folder(&folder)?;
        let keys = match rows {
            0 => Vec::new(),
            _ => match (unloaded_files(reader, &folder).await?, &downloads) {
                (Unloaded::External { files, .. }, _) => files,
                (internal, Some(local)) => internal.download(reader, local.path()).await?,
                (Unloaded::Internal { stage_path, .. }, None) => {
                    return Err(SnowflakeSourceError::Invalid(format!(
                        "{stage_path} is an internal stage with no local folder to download into"
                    ))
                    .into());
                }
            },
        };
        let files = UnloadedFiles {
            folder: location,
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
