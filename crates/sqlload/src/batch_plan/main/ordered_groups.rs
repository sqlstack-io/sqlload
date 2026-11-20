//! Cut a files source's files into consecutive batches, keeping their order.

use crate::batch_plan::constants::{BATCH_BYTES, MAX_BATCH_FILES};
use crate::batch_plan::models::FileGroup;
use crate::files_source::models::DataFile;

/// `files` in consecutive batches, in the order given, each closed at 1,000 files or about 1 GB of files, whichever comes first (a single larger file is a batch by itself): a files source's merge, or `delete_insert` by `replace_key`, which applies the files in order, the newest file winning a key repeated across them.
pub fn ordered_groups(files: &[DataFile]) -> Vec<FileGroup> {
    let mut groups: Vec<FileGroup> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut bytes: usize = 0;
    for (index, data) in files.iter().enumerate() {
        let file_bytes = data.file.size as usize;
        if !current.is_empty()
            && (current.len() == MAX_BATCH_FILES || bytes + file_bytes > BATCH_BYTES)
        {
            groups.push(FileGroup {
                files: std::mem::take(&mut current),
                values: Vec::new(),
            });
            bytes = 0;
        }
        current.push(index);
        bytes += file_bytes;
    }
    if !current.is_empty() {
        groups.push(FileGroup {
            files: current,
            values: Vec::new(),
        });
    }
    groups
}
