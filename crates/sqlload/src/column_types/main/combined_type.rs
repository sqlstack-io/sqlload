//! The one type for a column whose values suggested two types.

use crate::column_types::models::ColumnType;

/// One type for a column that held values of both types: int and float make float; any other mix is text.
pub fn combined_type(seen: ColumnType, value: ColumnType) -> ColumnType {
    match (seen, value) {
        (seen, value) if seen == value => seen,
        (ColumnType::Int | ColumnType::Float, ColumnType::Int | ColumnType::Float) => {
            ColumnType::Float
        }
        _ => ColumnType::Text,
    }
}
