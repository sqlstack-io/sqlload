//! Where load files sit in the staging volume.

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::databricks::constants::STAGE_FOLDER;
use crate::databricks_api::models::DirectoryEntry;

/// `/Volumes/c/s/v` for `c.s.v`.
pub(crate) fn volume_path(volume: &str) -> String {
    let parts: Vec<&str> = volume.split('.').collect();
    format!("/Volumes/{}", parts.join("/"))
}

/// The folder of one load, `<volume>/sqlload/<load id>`.
pub(crate) fn load_folder(volume: &str, load_id: &str) -> String {
    format!("{volume}/{STAGE_FOLDER}/{load_id}")
}

/// The folder of the batch `target` fills, `<volume>/sqlload/<load id>/<batch>`.
pub(crate) fn batch_folder(volume: &str, target: &WriteTarget<'_>) -> String {
    format!(
        "{}/{:05}",
        load_folder(volume, target.load_id),
        target.batch
    )
}

/// The load folders among `listed` (each folder of `<volume>/sqlload` with the files under it) whose newest file is older than `before_ms`, or which hold no file at all; a load still writing keeps its folder.
pub(crate) fn stale_folders(
    listed: &[(DirectoryEntry, Vec<DirectoryEntry>)],
    before_ms: i64,
) -> Vec<String> {
    let mut stale: Vec<String> = Vec::new();
    for (folder, files) in listed.iter().filter(|(folder, _)| folder.is_directory) {
        let newest = files.iter().filter_map(|file| file.last_modified_ms).max();
        if newest.is_none_or(|newest| newest < before_ms) {
            stale.push(folder.path.trim_end_matches('/').to_owned());
        }
    }
    stale
}
