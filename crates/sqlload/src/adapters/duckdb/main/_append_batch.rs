//! Append an Arrow batch to a table.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::models::DuckDbSql;
use arrow_array::RecordBatch;
use duckdb::vtab::ArrowBatchRegistration;

use crate::adapters::contract::models::TableName;
use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::_append_batch::append_batch as bridge_append;

/// Insert `batch` into `table` by column name, inside the connection's open transaction if it has one: columns the batch lacks get their `DEFAULT`, and values are converted to the column's type, so bad JSON or UUIDs fail here; a column among `variants` (a `VARIANT` column) takes each value as JSON, so objects, arrays, numbers and booleans keep their types. MotherDuck's bridge `COPY`s the batch from a zstd Parquet file instead, its `VARIANT` columns typed `JSON` there.
pub(in crate::adapters) async fn append_batch(
    destination: &DuckDbDestination,
    (table, variants): (&TableName, &[&str]),
    batch: RecordBatch,
) -> Result<(), DuckDbError> {
    let target = DuckDbSql.render_qualified_name(table);
    let schema = batch.schema();
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| DuckDbSql.render_identifier(field.name()))
        .collect();
    let values: Vec<String> = schema
        .fields()
        .iter()
        .zip(&names)
        .map(
            |(field, name)| match variants.contains(&field.name().as_str()) {
                true => format!("CAST({name} AS JSON)"),
                false => name.clone(),
            },
        )
        .collect();
    let (list, values) = (names.join(", "), values.join(", "));
    if let DuckDbConnection::Bridge(bridge) = &destination.connection {
        let copy = format!("COPY {target} ({list}) FROM ? (FORMAT parquet)");
        let json: Vec<String> = variants.iter().map(|name| (*name).to_owned()).collect();
        return Ok(bridge_append(bridge, copy, (batch, json)).await?);
    }
    with_connection(destination, move |connection| {
        connection
            .prepare(&format!(
                "INSERT INTO {target} ({list}) SELECT {values} FROM system.main.arrow(?)"
            ))?
            .execute([&ArrowBatchRegistration::new(batch)])?;
        Ok(())
    })
    .await
}
