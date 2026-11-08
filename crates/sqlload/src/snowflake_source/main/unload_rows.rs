//! Unload a table or query as Parquet and stream it back as Arrow batches.

use arrow_array::RecordBatch;

use crate::snowflake_source::_helpers::convert::output_schema;
use crate::snowflake_source::_helpers::folders::{remove_folder, unload_folder, unloaded_files};
use crate::snowflake_source::_helpers::unload::{copy_statement, read_unloaded, rows_unloaded};
use crate::snowflake_source::_helpers::unload_query::unload_query;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::{SnowflakeReader, UnloadRequest};
use crate::sql_api::main::execute_sql::execute_sql;

/// Unload `request` into a folder of its own with `COPY INTO`, fetch the files several at a time and pass their rows to `on_batch` in batches of at most `batch_rows`, then remove the folder, after a failure too (a folder that cannot be removed is only a warning); returns the row count.
pub async fn unload_rows(
    reader: &SnowflakeReader,
    request: &UnloadRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, SnowflakeSourceError> {
    let query = unload_query(request).map_err(SnowflakeSourceError::Invalid)?;
    let schema = output_schema(request.columns).map_err(SnowflakeSourceError::Invalid)?;
    let run = uuid::Uuid::new_v4().simple().to_string();
    let folder = unload_folder(reader, &run).await?;
    let (target, copy) = copy_statement(&folder, &query);
    let read = async {
        if rows_unloaded(&execute_sql(&reader.sql, &copy).await?)? == 0 {
            return Ok(0);
        }
        let files = unloaded_files(reader, &folder).await?;
        read_unloaded(reader, &files, (&schema, request.batch_rows), &mut on_batch).await
    }
    .await;
    if let Err(error) = remove_folder(reader, &folder).await {
        tracing::warn!(folder = %target, %error, "cannot remove the unloaded files");
    }
    read
}
