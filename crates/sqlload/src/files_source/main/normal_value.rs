//! Values from files' paths, in one form.

use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::plan::partitions::normalised;

/// `value`, a Hive folder's or a capture's text, converted to `column_type` as SQLLoad's reader converts it and printed back in the one form every destination reads alike; `Err` when it does not convert.
pub fn normal_value(value: &str, column_type: &ColumnType) -> Result<String, String> {
    normalised(value, column_type)
}
