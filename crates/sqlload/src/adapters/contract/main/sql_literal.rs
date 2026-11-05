//! A string literal.

use crate::column_types::models::Warehouse;
use crate::adapters::contract::_helpers::identifiers::literal;

/// `text` as a string literal `warehouse` reads back unchanged.
pub fn sql_literal(text: &str, warehouse: Warehouse) -> String {
    literal(text, warehouse)
}
