//! Compare two cursor positions.

use std::cmp::Ordering;

use crate::column_types::models::ColumnType;

/// The later of two positions in the text SQLLoad writes cursors as (fixed-width timestamps and dates, which sort as text; numbers compared as numbers).
pub fn later_cursor(
    left: Option<String>,
    right: Option<String>,
    column_type: &ColumnType,
) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => match cursor_order(&left, &right, column_type) {
            Ordering::Less => Some(right),
            _ => Some(left),
        },
        (left, right) => left.or(right),
    }
}

fn cursor_order(left: &str, right: &str, column_type: &ColumnType) -> Ordering {
    let numeric = matches!(
        column_type,
        ColumnType::Int | ColumnType::SmallInt | ColumnType::Float | ColumnType::Decimal { .. }
    );
    match (numeric, left.parse::<f64>(), right.parse::<f64>()) {
        (true, Ok(left), Ok(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        _ => left.cmp(right),
    }
}
