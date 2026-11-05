//! A table name as SQL.

use crate::column_types::models::Warehouse;
use crate::adapters::contract::_helpers::identifiers::qualified;
use crate::adapters::contract::models::TableName;

/// The quoted, fully qualified name of `table`.
pub fn qualified_name(table: &TableName, warehouse: Warehouse) -> String {
    qualified(table, warehouse)
}
