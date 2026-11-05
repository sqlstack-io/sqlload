//! Group a files source's files into batches.

use crate::batch_plan::constants::MAX_BATCH_FILES;
use crate::batch_plan::models::FileGroup;
use crate::files_source::models::DataFile;

/// Whole partitions in key order, up to 1,000 files a batch; a partition of more files is split across batches, except when the batch replaces partitions by the cursor's value (`by_value`, `delete_insert`): then a value's files always commit together, however many.
pub fn file_groups(files: &[DataFile], by_value: Option<&str>) -> Vec<FileGroup> {
    let key = |data: &DataFile| match by_value {
        Some(column) => data
            .partitions
            .iter()
            .filter(|(name, _)| name == column)
            .map(|(_, value)| value.clone())
            .collect::<Vec<String>>(),
        None => data
            .partitions
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect(),
    };
    let mut partitions: Vec<(Vec<String>, Vec<usize>)> = Vec::new();
    for (index, data) in files.iter().enumerate() {
        let value = key(data);
        match partitions.last_mut() {
            Some((last, indexes)) if *last == value => indexes.push(index),
            _ => partitions.push((value, vec![index])),
        }
    }
    let mut groups: Vec<FileGroup> = Vec::new();
    let mut current = FileGroup {
        files: Vec::new(),
        values: Vec::new(),
    };
    for (value, indexes) in partitions {
        if !current.files.is_empty() && current.files.len() + indexes.len() > MAX_BATCH_FILES {
            groups.push(std::mem::replace(
                &mut current,
                FileGroup {
                    files: Vec::new(),
                    values: Vec::new(),
                },
            ));
        }
        match by_value {
            Some(_) => {
                current.values.extend(value);
                current.files.extend(indexes);
            }
            None => {
                for chunk in indexes.chunks(MAX_BATCH_FILES) {
                    if current.files.len() + chunk.len() > MAX_BATCH_FILES {
                        groups.push(std::mem::replace(
                            &mut current,
                            FileGroup {
                                files: Vec::new(),
                                values: Vec::new(),
                            },
                        ));
                    }
                    current.files.extend(chunk);
                }
            }
        }
    }
    if !current.files.is_empty() {
        groups.push(current);
    }
    groups
}
