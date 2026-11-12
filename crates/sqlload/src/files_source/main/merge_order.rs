//! The order a merge applies files in.

use crate::files_source::models::DataFile;

/// `files` oldest modified first, files modified at the same time in path order, so a key repeated across files ends with the newest file's value.
pub fn merge_order(mut files: Vec<DataFile>) -> Vec<DataFile> {
    files.sort_by(|left, right| {
        (left.file.modified_ms, &left.file.relative_key)
            .cmp(&(right.file.modified_ms, &right.file.relative_key))
    });
    files
}
