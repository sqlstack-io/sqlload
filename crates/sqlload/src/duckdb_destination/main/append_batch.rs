//! Append an Arrow batch to a table.

use arrow_array::RecordBatch;
use duckdb::vtab::ArrowBatchRegistration;

use crate::column_types::models::Warehouse;
use crate::duckdb_destination::_helpers::blocking::with_connection;
use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::quoted_identifier::quoted_identifier;
use crate::write_sql::models::TableName;

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
                "INSERT INTO {target} ({list}) SELECT {list} FROM arrow(?)"
            ))?
            .execute([&ArrowBatchRegistration::new(batch)])?;
        Ok(())
    })
    .await
}
