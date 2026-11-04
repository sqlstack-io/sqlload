//! List the distinct values of one column.

use crate::column_types::models::Warehouse;
use crate::write_sql::_helpers::identifiers::{qualified, quote};
use crate::write_sql::models::TableName;

/// `SELECT DISTINCT column FROM table`.
pub fn distinct_query(table: &TableName, column: &str, warehouse: Warehouse) -> String {
    format!(
        "SELECT DISTINCT {} FROM {}",
        quote(column, warehouse),
        qualified(table, warehouse)
    )
}
