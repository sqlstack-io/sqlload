//! Whether the planned read of a files table's own files loads exactly when the destination reads them itself.

use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::models::{FileColumn, FilesRead, StoredRead};
use crate::loader::models::FilesSource;
use crate::schema_plan::models::DesiredColumn;

/// Why a destination reading `files` itself would not load the planned `read` exactly, whatever the destination: a column held in a way a plain cast would not keep, a read column loaded as another type than the files hold, or a path its reader would take as a pattern; `None` when none is.
pub(in crate::adapters) fn files_direct_problem(
    files: &FilesSource,
    read: &FilesRead,
) -> Option<String> {
    files
        .table
        .columns
        .iter()
        .find_map(held_as)
        .or_else(|| {
            read.columns
                .iter()
                .find_map(|column| retyped(files, column))
        })
        .or_else(|| {
            read.files.iter().find_map(|data| {
                let path = format!("{}{}", files.location, data.file.relative_key);
                path.contains(['*', '?', '[', '{'])
                    .then(|| format!("the path {path} reads as a pattern"))
            })
        })
}

/// How `column` is held when a plain cast would not load it exactly, as a problem.
fn held_as(column: &FileColumn) -> Option<String> {
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
fn retyped(files: &FilesSource, column: &DesiredColumn) -> Option<String> {
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
