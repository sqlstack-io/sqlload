//! The DuckDB-only checks that decide whether DuckDB may read a files table's files itself.

use crate::files_source::models::FileColumn;

/// A file column whose name DuckDB's file reader uses itself, in any case: such a column would take the place of `file_index`, which picks each row's per-file values.
pub(crate) fn reserved_name(column: &FileColumn) -> Option<String> {
    ["filename", "file_row_number", "file_index"]
        .iter()
        .any(|reserved| column.name.eq_ignore_ascii_case(reserved))
        .then(|| {
            format!(
                "the files have a column named {}, which DuckDB's file reader adds itself",
                column.name
            )
        })
}
