//! Append an Arrow batch to a table.

use arrow_array::RecordBatch;

use crate::duckdb_destination::_helpers::blocking::with_connection;
use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;
use crate::write_sql::models::TableName;

/// Append `batch` to `table` by column name (the table's other columns get NULL), inside the connection's open transaction if it has one.
pub async fn append_batch(
    destination: &DuckDbDestination,
    table: &TableName,
    batch: RecordBatch,
) -> Result<(), DuckDbError> {
    let schema = table.schema.clone();
    let name = table.table.clone();
    with_connection(destination, move |connection| {
        let fields = batch.schema();
        let columns: Vec<&str> = fields
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let mut appender = connection.appender_with_columns_to_db(&name, &schema, &columns)?;
        appender.append_record_batch(batch.clone())?;
        appender.flush()?;
        Ok(())
    })
    .await
}
