//! Pick the files of a partition's newest load.

use std::collections::BTreeMap;

use crate::adapters::files::_helpers::layout::loads::load_id_of;
use crate::file_store::models::StoredFile;

/// The files the newest load wrote (by load id in the name; a file of another shape is a load of its own), the newest load being the one with the latest modified file; loads tied at that time are all read.
pub fn newest_load_files(files: &[StoredFile]) -> Vec<StoredFile> {
    let mut loads: BTreeMap<&str, Vec<&StoredFile>> = BTreeMap::new();
    for file in files {
        let load = load_id_of(&file.relative_key).unwrap_or(&file.relative_key);
        loads.entry(load).or_default().push(file);
    }
    let mut latest: BTreeMap<&str, i64> = BTreeMap::new();
    for (load, files) in &loads {
        let modified = files
            .iter()
            .map(|file| file.modified_ms)
            .max()
            .unwrap_or_default();
        latest.insert(load, modified);
    }
    let newest = latest.values().copied().max().unwrap_or_default();
    let mut chosen: Vec<StoredFile> = Vec::new();
    for (load, files) in loads {
        if latest.get(load) == Some(&newest) {
            chosen.extend(files.into_iter().cloned());
        }
    }
    chosen
}
