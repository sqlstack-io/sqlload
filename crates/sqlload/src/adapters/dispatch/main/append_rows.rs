//! Append Arrow batches to a table.

use arrow_array::RecordBatch;

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_append_batch::append_batch;
use crate::loader::errors::LoaderError;

/// Append `batch` to `table` by column name (DuckDB, which takes Arrow directly).
pub async fn append_rows(
    destination: &Destination,
    table: &TableName,
    batch: RecordBatch,
) -> Result<(), LoaderError> {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(append_batch(duckdb, table, batch).await?),
        _ => Err(LoaderError::Invalid(
            "only DuckDB takes Arrow batches directly".to_owned(),
        )),
    }
}
