//! Whether a commit ending at a cursor value leaves whole partitions.

use std::collections::BTreeSet;

use crate::column_types::models::ColumnType;
use crate::files_destination::_helpers::partitions::{bound_key, partition_key};
use crate::files_destination::models::PartitionBy;

/// Whether every partition in `written` lies wholly below `to`: rows for it can no longer follow, so a commit may replace its old files. A `batch` finer than the partition ends inside one.
pub fn partitions_finished(
    written: &BTreeSet<String>,
    by: PartitionBy,
    cursor_type: &ColumnType,
    to: &str,
) -> Result<bool, String> {
    let end = bound_key(to, by, cursor_type)?;
    for value in written {
        if partition_key(value, by, cursor_type)? >= end {
            return Ok(false);
        }
    }
    Ok(true)
}
