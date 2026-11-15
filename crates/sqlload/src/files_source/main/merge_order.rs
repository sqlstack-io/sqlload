//! The order a merge applies files in.

use crate::files_source::models::{DataFile, FilesOrder};

/// `files` in `order`: oldest modified first, files modified at the same time in path order; or by path alone, in code point order. A key repeated across files ends with the last file's value.
pub fn merge_order(mut files: Vec<DataFile>, order: FilesOrder) -> Vec<DataFile> {
    match order {
        FilesOrder::Modified => files.sort_by(|left, right| {
            (left.file.modified_ms, &left.file.relative_key)
                .cmp(&(right.file.modified_ms, &right.file.relative_key))
        }),
        FilesOrder::Path => {
            files.sort_by(|left, right| left.file.relative_key.cmp(&right.file.relative_key));
        }
    }
    files
}
