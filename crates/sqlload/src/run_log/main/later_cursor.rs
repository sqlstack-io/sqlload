//! Compare two cursor positions.

use std::cmp::Ordering;

use crate::column_types::models::ColumnType;
use crate::run_log::main::cursor_order::cursor_order;

/// The later of two positions in the text SQLLoad writes cursors as (compared by [`cursor_order`]).
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
