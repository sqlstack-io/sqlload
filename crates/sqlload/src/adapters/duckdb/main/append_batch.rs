//! Append an Arrow batch to a table.

use arrow_array::RecordBatch;
use duckdb::vtab::ArrowBatchRegistration;

use crate::column_types::models::Warehouse;
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::main::quoted_identifier::quoted_identifier;
use crate::adapters::contract::models::TableName;

/// Insert `batch` into `table` by column name, inside the connection's open transaction if it has one: columns the batch lacks get their `DEFAULT`, and values are converted to the column's type, so bad JSON or UUIDs fail here.
pub async fn append_batch(
    destination: &DuckDbDestination,
    table: &TableName,
    batch: RecordBatch,
) -> Result<(), DuckDbError> {
    let target = qualified_name(table, Warehouse::DuckDb);
    with_connection(destination, move |connection| {
        let fields = batch.schema();
        let columns: Vec<String> = fields
            .fields()
            .iter()
            .map(|field| quoted_identifier(field.name(), Warehouse::DuckDb))
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
