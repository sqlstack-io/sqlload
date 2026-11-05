//! Append an Arrow batch to a table.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::models::DuckDbSql;
use arrow_array::RecordBatch;
use duckdb::vtab::ArrowBatchRegistration;

use crate::adapters::contract::models::TableName;
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// Insert `batch` into `table` by column name, inside the connection's open transaction if it has one: columns the batch lacks get their `DEFAULT`, and values are converted to the column's type, so bad JSON or UUIDs fail here.
pub(in crate::adapters) async fn append_batch(
    destination: &DuckDbDestination,
    table: &TableName,
    batch: RecordBatch,
) -> Result<(), DuckDbError> {
    let target = DuckDbSql.render_qualified_name(table);
    with_connection(destination, move |connection| {
        let fields = batch.schema();
        let columns: Vec<String> = fields
            .fields()
            .iter()
            .map(|field| DuckDbSql.render_identifier(field.name()))
            .collect();
        let list = columns.join(", ");
        connection
            .prepare(&format!(
                "INSERT INTO {target} ({list}) SELECT {list} FROM system.main.arrow(?)"
            ))?
            .execute([&ArrowBatchRegistration::new(batch)])?;
        Ok(())
    })
    .await
}
