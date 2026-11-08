//! Unload a table or query as Parquet and stream it back as Arrow batches.

use arrow_array::RecordBatch;

use crate::snowflake_source::_helpers::convert::output_schema;
use crate::snowflake_source::_helpers::folders::{remove_folder, unload_folder, unloaded_files};
use crate::snowflake_source::_helpers::unload::{copy_statement, read_unloaded, rows_unloaded};
use crate::snowflake_source::_helpers::unload_query::unload_query;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::{SnowflakeReader, UnloadRequest};
use crate::sql_api::main::execute_sql::execute_sql;

/// Unload `request` into a folder of its own with `COPY INTO`, check the files hold every row it reports, fetch the files several at a time and pass their rows to `on_batch` in batches of at most `batch_rows`, then remove the folder, after a failure too (a folder that cannot be removed is only a warning); returns the row count.
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
        let t=std::time::Instant::now();
        let unloaded = rows_unloaded(&execute_sql(&reader.sql, &copy).await?)?;
        eprintln!("PROF copy {:?}", t.elapsed());
        if unloaded == 0 {
            return Ok(0);
        }
        let t=std::time::Instant::now();
        let files = unloaded_files(reader, &folder).await?;
        eprintln!("PROF list {:?} files={}", t.elapsed(), files.len());
        let t=std::time::Instant::now();
        let read =
            read_unloaded(reader, &files, (&schema, request.batch_rows), &mut on_batch).await?;
        eprintln!("PROF read {:?} batch_rows={}", t.elapsed(), request.batch_rows);
        match read == unloaded {
            true => Ok(read),
            false => Err(SnowflakeSourceError::Invalid(format!(
                "COPY INTO unloaded {unloaded} rows to {target}, but its files hold {read}"
            ))),
        }
    }
    .await;
    let t=std::time::Instant::now();
    let removed = remove_folder(reader, &folder).await;
    eprintln!("PROF remove {:?}", t.elapsed());
    if let Err(error) = removed {
        tracing::warn!(folder = %target, %error, "cannot remove the unloaded files");
    }
    read
}
