//! The DuckDB-only checks that decide whether DuckDB may read a files table's files itself.

use crate::files_source::models::FileColumn;
use crate::store_paths::models::StoreKind;

/// Why an S3 store points somewhere DuckDB would not read: an endpoint in the connection or in `AWS_ENDPOINT` / `AWS_ENDPOINT_URL`, which SQLLoad's reader follows.
pub(crate) fn custom_endpoint(store: &StoreKind) -> Option<String> {
    let from_env = std::env::var_os("AWS_ENDPOINT").is_some()
        || std::env::var_os("AWS_ENDPOINT_URL").is_some();
    match store {
        StoreKind::S3 {
            endpoint: Some(_), ..
        } => Some("the S3 connection sets a custom endpoint".to_owned()),
        StoreKind::S3 { .. } if from_env => {
            Some("AWS_ENDPOINT or AWS_ENDPOINT_URL sets a custom S3 endpoint".to_owned())
        }
        _ => None,
    }
}

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
