//! Pick the files of a partition's newest load.

use std::collections::BTreeMap;

use crate::file_store::models::StoredFile;
use crate::files_destination::_helpers::loads::load_id_of;

/// The files the newest load wrote (by load id in the name; a file of another shape is a load of its own), the newest load being the one with the latest modified file.
pub fn newest_load_files(files: &[StoredFile]) -> Vec<StoredFile> {
    let mut loads: BTreeMap<&str, Vec<&StoredFile>> = BTreeMap::new();
    for file in files {
        let load = load_id_of(&file.relative_key).unwrap_or(&file.relative_key);
        loads.entry(load).or_default().push(file);
    }
    let mut newest: Option<(i64, &str)> = None;
    for (load, files) in &loads {
        let modified = files
            .iter()
            .map(|file| file.modified_ms)
            .max()
            .unwrap_or_default();
        if newest.is_none_or(|(latest, _)| modified >= latest) {
            newest = Some((modified, load));
        }
    }
    let chosen: Vec<&StoredFile> = newest
        .and_then(|(_, load)| loads.remove(load))
        .unwrap_or_default();
    chosen.into_iter().cloned().collect()
}
