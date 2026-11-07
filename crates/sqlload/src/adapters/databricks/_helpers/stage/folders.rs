//! Where load files sit in the staging volume.

use crate::adapters::contract::models::WriteTarget;
use crate::adapters::databricks::constants::STAGE_FOLDER;
use crate::adapters::databricks::models::{ListedLoad, StaleRemovals};

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

/// What to remove of `listed`: the files of each load whose newest file is older than `before_ms`, and the folders of those loads and of loads holding no file, deepest first. A load with a newer file keeps everything. The folders are only removed while empty, so a load that starts writing into one after the listing keeps it, and no file the listing did not show as old is ever deleted.
pub(crate) fn stale_removals(listed: &[ListedLoad], before_ms: i64) -> StaleRemovals {
    let mut removals = StaleRemovals::default();
    for load in listed {
        let newest = load
            .files
            .iter()
            .filter_map(|file| file.last_modified_ms)
            .max();
        let undated = load
            .files
            .iter()
            .any(|file| file.last_modified_ms.is_none());
        match newest {
            Some(newest) if newest >= before_ms || undated => continue,
            Some(_) => removals
                .files
                .extend(load.files.iter().map(|file| file.path.clone())),
            None if !load.files.is_empty() => continue,
            None => {}
        }
        removals.folders.extend(
            load.batches
                .iter()
                .map(|batch| batch.trim_end_matches('/').to_owned()),
        );
        removals
            .folders
            .push(load.folder.trim_end_matches('/').to_owned());
    }
    removals
}
