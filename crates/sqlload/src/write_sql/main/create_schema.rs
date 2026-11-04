//! Create the schema a table lives in.

use crate::column_types::models::Warehouse;
use crate::write_sql::_helpers::identifiers::quote;
use crate::write_sql::models::TableName;

/// `CREATE SCHEMA IF NOT EXISTS` for `table`'s schema.
pub fn create_schema(table: &TableName, warehouse: Warehouse) -> String {
    let schema = quote(&table.schema, warehouse);
    match &table.database {
        Some(database) => format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{schema}",
            quote(database, warehouse)
        ),
        None => format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
    }
}
