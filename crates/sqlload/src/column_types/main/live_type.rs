//! Read a live column's type back into the portable vocabulary.

use crate::column_types::_helpers::live_names;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse};

/// The portable type of a live column; `native(...)` when it has no portable equivalent.
pub fn live_type(live: &LiveColumnType, warehouse: Warehouse) -> ColumnType {
    let portable = match warehouse {
        Warehouse::Snowflake => live_names::snowflake(live),
        Warehouse::DuckDb | Warehouse::MotherDuck => live_names::duckdb(live),
        _ => None,
    };
    portable.unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
}
