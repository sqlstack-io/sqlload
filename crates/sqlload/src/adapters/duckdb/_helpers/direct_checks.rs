//! The checks that decide whether DuckDB may read a files table's files itself.

use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::models::{DataFile, FileColumn, StoredRead};
use crate::loader::models::FilesSource;
use crate::schema_plan::models::DesiredColumn;
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

/// A file column whose name DuckDB's file reader uses itself, in any case.
pub(crate) fn reserved_name(column: &FileColumn) -> Option<String> {
    ["filename", "file_row_number"]
        .iter()
        .any(|reserved| column.name.eq_ignore_ascii_case(reserved))
        .then(|| {
            format!(
                "the files have a column named {}, which DuckDB's file reader adds itself",
                column.name
            )
        })
}

/// How `column` is held when a plain cast would not load it exactly, as a problem.
pub(crate) fn held_as(column: &FileColumn) -> Option<String> {
    let held = match (
        &column.column_type,
        &column.direct_refusal,
        column.stored_read,
    ) {
        (Err(reason), _, _) => Some(reason.clone()),
        (Ok(_), Some(arrow_type), _) => Some(arrow_type.clone()),
        (Ok(_), None, StoredRead::Plain) => None,
        (Ok(_), None, StoredRead::Nanos) => Some("nanoseconds".to_owned()),
        (Ok(_), None, StoredRead::Epoch(_)) => Some("an epoch integer".to_owned()),
    };
    Some(format!("column {} is held as {}", column.name, held?))
}

/// A read column the files hold as another type than it loads as; SQLLoad's own values (file name, version, row, partitions) are not the files'.
pub(crate) fn retyped(files: &FilesSource, column: &DesiredColumn) -> Option<String> {
    let own = [SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN]
        .contains(&column.name.as_str())
        || files
            .partition_types
            .iter()
            .any(|(partition, _)| *partition == column.name);
    let held = files
        .table
        .columns
        .iter()
        .find(|found| found.name == column.name)?;
    match &held.column_type {
        Ok(file_type) if !own && *file_type != column.column_type => Some(format!(
            "{} is {file_type} in the files but loads as {}; SQLLoad converts it",
            column.name, column.column_type
        )),
        _ => None,
    }
}

/// A file whose path DuckDB would read as a glob pattern.
pub(crate) fn pattern(files: &FilesSource, data: &DataFile) -> Option<String> {
    let path = format!("{}{}", files.location, data.file.relative_key);
    path.contains(['*', '?', '[', '{'])
        .then(|| format!("the path {path} reads as a pattern"))
}
