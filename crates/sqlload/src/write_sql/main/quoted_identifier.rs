//! An identifier as SQL.

use crate::column_types::models::Warehouse;
use crate::write_sql::_helpers::identifiers::quote;

/// `name` quoted for `warehouse`, plain snake_case upper-cased for Snowflake as an unquoted name is stored.
pub fn quoted_identifier(name: &str, warehouse: Warehouse) -> String {
    quote(name, warehouse)
}
