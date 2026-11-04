//! Combine the schemas of several Parquet footers.

use crate::files_source::_helpers::columns::merge_schema;
use crate::files_source::models::{FileColumn, Footer};

/// Every column in `footers` (first footer first) with its portable type, and a problem for each column whose type differs between files.
pub fn footer_columns(footers: &[Footer]) -> (Vec<FileColumn>, Vec<String>) {
    let mut columns: Vec<FileColumn> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    for footer in footers {
        let (merged, conflicts) = merge_schema(columns, &footer.schema, &footer.relative_key, &[]);
        columns = merged;
        problems.extend(conflicts);
    }
    (columns, problems)
}
