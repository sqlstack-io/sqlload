//! Check a cursor window's bounds against the partitions.

use crate::adapters::files::_helpers::layout::partitions::bound_key;
use crate::adapters::files::models::PartitionBy;
use crate::column_types::models::ColumnType;

/// An error naming the first bound that cannot be compared with the partition values.
pub fn check_window(
    by: PartitionBy,
    cursor_type: &ColumnType,
    bounds: (Option<&str>, Option<&str>),
) -> Result<(), String> {
    for bound in [bounds.0, bounds.1].into_iter().flatten() {
        bound_key(bound, by, cursor_type)
            .map_err(|problem| format!("cursor bound {bound:?}: {problem}"))?;
    }
    Ok(())
}
