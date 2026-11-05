//! Create a destination table.

use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::Warehouse;
use crate::schema_plan::models::DesiredColumn;
use crate::adapters::contract::_helpers::identifiers::{qualified, quote};
use crate::adapters::contract::models::TableName;

/// `CREATE TABLE IF NOT EXISTS` with every column nullable.
pub fn create_table(table: &TableName, columns: &[DesiredColumn], warehouse: Warehouse) -> String {
    let definitions: Vec<String> = columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                quote(&column.name, warehouse),
                warehouse_type(&column.column_type, warehouse).sql
            )
        })
        .collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified(table, warehouse),
        definitions.join(", ")
    )
}
