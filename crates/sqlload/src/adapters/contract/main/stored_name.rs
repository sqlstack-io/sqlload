//! A name as the warehouse stores it.

use crate::column_types::models::Warehouse;
use crate::adapters::contract::_helpers::identifiers::quote;

/// `name` as the warehouse reports it in its information schema (upper case for plain names on Snowflake).
pub fn stored_name(name: &str, warehouse: Warehouse) -> String {
    let quoted = quote(name, warehouse);
    quoted[1..quoted.len() - 1].replace("\"\"", "\"")
}
