//! Decide whether a partition lies in a cursor window.

use crate::column_types::models::ColumnType;
use crate::files_destination::_helpers::partitions::{bound_key, partition_key};
use crate::files_destination::models::PartitionBy;

/// Whether partition `value` lies in `from <= value < to` (each open when `None`).
pub fn partition_in_window(
    value: &str,
    by: PartitionBy,
    cursor_type: &ColumnType,
    bounds: (Option<&str>, Option<&str>),
) -> Result<bool, String> {
    let key = partition_key(value, by, cursor_type)?;
    let after_start = match bounds.0 {
        Some(from) => key >= bound_key(from, by, cursor_type)?,
        None => true,
    };
    let before_end = match bounds.1 {
        Some(to) => key < bound_key(to, by, cursor_type)?,
        None => true,
    };
    Ok(after_start && before_end)
}
