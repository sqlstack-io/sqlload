//! Append an Arrow batch to a staging table.

use arrow_array::RecordBatch;

use crate::duckdb_destination::_helpers::blocking::with_connection;
use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;
use crate::write_sql::models::TableName;

/// Append `batch` to `table`, whose columns match the batch's in order.
pub async fn append_batch(
    destination: &DuckDbDestination,
    table: &TableName,
    batch: RecordBatch,
) -> Result<(), DuckDbError> {
    let schema = table.schema.clone();
    let name = table.table.clone();
    with_connection(destination, move |connection| {
        let mut appender = connection.appender_to_db(&name, &schema)?;
        appender.append_record_batch(batch)?;
        appender.flush()?;
        Ok(())
    })
    .await
}
