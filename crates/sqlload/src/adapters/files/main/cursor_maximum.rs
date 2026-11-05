//! Read a cursor's maximum from Parquet statistics.

use arrow_array::ArrayRef;

use crate::adapters::files::_helpers::write::statistics::{largest, row_group_maxes};
use crate::column_types::models::ColumnType;
use crate::files_source::models::Footer;

/// The largest `column` value in `footers`' row group statistics, as cursor bound text takes it (epoch microseconds for timestamps); `None` when the files hold no rows.
pub fn cursor_maximum(
    footers: &[Footer],
    column: &str,
    cursor_type: &ColumnType,
) -> Result<Option<String>, String> {
    let maxes: Vec<ArrayRef> = footers
        .iter()
        .map(|footer| row_group_maxes(footer, column))
        .collect::<Result<_, String>>()?;
    largest(&maxes, cursor_type).map_err(|error| format!("{column}: {error}"))
}
