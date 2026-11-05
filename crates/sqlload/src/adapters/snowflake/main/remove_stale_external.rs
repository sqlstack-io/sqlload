//! Clean up the load folders failed runs left under an external stage's URL.

use std::collections::BTreeMap;

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::adapters::snowflake::constants::{EXTERNAL_STAGE_FOLDER, STALE_STAGE_HOURS};
use crate::adapters::snowflake::models::ExternalStage;
use crate::store_paths::models::Location;

/// Delete every load folder under `_sqlload/` below the stage URL whose files are all older than a day; returns how many, with SQLLoad's own S3 client.
pub async fn remove_stale_external(stage: &ExternalStage) -> Result<usize, String> {
    let folder = open_folder(&Location {
        store: stage.location.store.clone(),
        prefix: format!("{}{EXTERNAL_STAGE_FOLDER}/", stage.location.prefix),
    })
    .map_err(|error| error.to_string())?;
    let files = list_files(&folder)
        .await
        .map_err(|error| error.to_string())?;
    let before =
        (chrono::Utc::now() - chrono::Duration::hours(STALE_STAGE_HOURS)).timestamp_millis();
    let mut newest: BTreeMap<&str, (i64, Vec<String>)> = BTreeMap::new();
    for file in &files {
        let load = file.relative_key.split('/').next().unwrap_or_default();
        let entry = newest.entry(load).or_default();
        entry.0 = entry.0.max(file.modified_ms);
        entry.1.push(file.relative_key.clone());
    }
    let mut removed: usize = 0;
    for (newest_ms, keys) in newest.values() {
        if *newest_ms < before {
            delete_files(&folder, keys)
                .await
                .map_err(|error| error.to_string())?;
            removed += 1;
        }
    }
    Ok(removed)
}
