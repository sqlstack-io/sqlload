//! A table name as `plan` prints it.

use crate::column_types::models::Warehouse;
use crate::write_sql::models::TableName;

/// `SCHEMA.TABLE` the way the warehouse stores the names (upper case on Snowflake).
pub fn display_name(table: &TableName, warehouse: Warehouse) -> String {
    let name = format!("{}.{}", table.schema, table.table);
    match warehouse {
        Warehouse::Snowflake => name.to_ascii_uppercase(),
        _ => name,
    }
}
