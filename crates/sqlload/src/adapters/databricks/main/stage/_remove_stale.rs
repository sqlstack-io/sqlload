//! Remove the load folders failed runs left in the staging volume.

use crate::adapters::databricks::_helpers::stage::folders::stale_removals;
use crate::adapters::databricks::constants::{STAGE_FOLDER, STALE_STAGE_HOURS};
use crate::adapters::databricks::models::{DatabricksDestination, ListedLoad};
use crate::databricks_api::main::delete_path::delete_path;
use crate::databricks_api::main::list_directory::list_directory;
use crate::http::errors::ApiError;
use crate::time_text::main::now_ms::now_ms;

/// Remove the files of load folders in `<volume>/sqlload` whose files are all over a day old, then those folders and empty ones while they stay empty; how many load folders went. A folder another load writes into meanwhile is kept, and a removal that fails only warns.
pub(in crate::adapters) async fn remove_stale(
    destination: &DatabricksDestination,
) -> Result<usize, ApiError> {
    let api = &destination.api;
    let root = format!("{}/{STAGE_FOLDER}", destination.volume);
    let Some(loads) = list_directory(api, &root).await? else {
        return Ok(0);
    };
    let mut listed: Vec<ListedLoad> = Vec::new();
    for load in loads.into_iter().filter(|load| load.is_directory) {
        let mut found = ListedLoad {
            folder: load.path.clone(),
            ..ListedLoad::default()
        };
        for entry in list_directory(api, &load.path).await?.unwrap_or_default() {
            match entry.is_directory {
                true => {
                    let files = list_directory(api, &entry.path).await?.unwrap_or_default();
                    found
                        .files
                        .extend(files.into_iter().filter(|file| !file.is_directory));
                    found.batches.push(entry.path);
                }
                false => found.files.push(entry),
            }
        }
        listed.push(found);
    }
    let removals = stale_removals(&listed, now_ms() - STALE_STAGE_HOURS * 3_600_000);
    for file in &removals.files {
        if let Err(error) = delete_path(api, file, false).await {
            tracing::warn!(%error, file, "cannot remove a stale staged file");
        }
    }
    let loads: Vec<&str> = listed
        .iter()
        .map(|load| load.folder.trim_end_matches('/'))
        .collect();
    let mut removed = 0;
    for folder in &removals.folders {
        let deleted = delete_path(api, folder, true).await;
        match (deleted, loads.contains(&folder.as_str())) {
            (Ok(()), true) => removed += 1,
            (Ok(()), false) => {}
            (Err(error), _) => {
                tracing::debug!(%error, folder, "a staged folder is not empty; kept")
            }
        }
    }
    Ok(removed)
}
