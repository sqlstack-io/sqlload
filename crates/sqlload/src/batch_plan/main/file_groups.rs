//! Group a files source's files into batches.

use std::collections::BTreeMap;

use crate::batch_plan::constants::{BATCH_BYTES, MAX_BATCH_FILES};
use crate::batch_plan::models::FileGroup;
use crate::files_source::models::DataFile;

/// Batches of whole partitions in key order, each closed at 1,000 files or about 1 GB of files, whichever comes first; a partition bigger than that is split, except when the batch replaces partitions by the cursor's value (`by_value`, `delete_insert`): then all of a value's files, wherever they are listed (`region=eu/day=d` and `region=us/day=d` alike), always commit together, however many.
pub fn file_groups(files: &[DataFile], by_value: Option<&str>) -> Vec<FileGroup> {
    let mut partitions: Vec<(Vec<String>, Vec<usize>)> = Vec::new();
    let mut by_key: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    for (index, data) in files.iter().enumerate() {
        let key = partition_key(data, by_value);
        match by_key.get(&key) {
            Some(position) => partitions[*position].1.push(index),
            None => {
                by_key.insert(key.clone(), partitions.len());
                partitions.push((key, vec![index]));
            }
        }
    }
    let size = |index: &usize| files.get(*index).map_or(0, |data| data.file.size as usize);
    let mut groups: Vec<FileGroup> = Vec::new();
    let mut current = FileGroup {
        files: Vec::new(),
        values: Vec::new(),
    };
    let mut bytes: usize = 0;
    for (key, indexes) in partitions {
        let whole = by_value.is_some();
        let pieces: Vec<Vec<usize>> = match whole {
            true => vec![indexes],
            false => split_partition(&indexes, &size),
        };
        for piece in pieces {
            let piece_bytes: usize = piece.iter().map(size).sum();
            let over = current.files.len() + piece.len() > MAX_BATCH_FILES
                || bytes + piece_bytes > BATCH_BYTES;
            if !current.files.is_empty() && over {
                groups.push(std::mem::replace(
                    &mut current,
                    FileGroup {
                        files: Vec::new(),
                        values: Vec::new(),
                    },
                ));
                bytes = 0;
            }
            current.files.extend(piece);
            bytes += piece_bytes;
        }
        if whole {
            current.values.extend(key);
        }
    }
    if !current.files.is_empty() {
        groups.push(current);
    }
    groups
}

/// What keeps files together: the cursor's partition value when replacing by value, else every partition value.
fn partition_key(data: &DataFile, by_value: Option<&str>) -> Vec<String> {
    match by_value {
        Some(column) => data
            .partitions
            .iter()
            .filter(|(name, _)| name == column)
            .map(|(_, value)| value.clone())
            .collect(),
        None => data
            .partitions
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect(),
    }
}

/// A partition's files in pieces of at most 1,000 files and about 1 GB (a single larger file is a piece by itself).
fn split_partition(indexes: &[usize], size: &impl Fn(&usize) -> usize) -> Vec<Vec<usize>> {
    let mut pieces: Vec<Vec<usize>> = Vec::new();
    let mut piece: Vec<usize> = Vec::new();
    let mut bytes: usize = 0;
    for index in indexes {
        let file_bytes = size(index);
        if !piece.is_empty() && (piece.len() == MAX_BATCH_FILES || bytes + file_bytes > BATCH_BYTES)
        {
            pieces.push(std::mem::take(&mut piece));
            bytes = 0;
        }
        piece.push(*index);
        bytes += file_bytes;
    }
    if !piece.is_empty() {
        pieces.push(piece);
    }
    pieces
}
