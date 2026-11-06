//! Remove the load folders failed runs left in the staging volume.

use crate::adapters::databricks::_helpers::stage::folders::stale_folders;
use crate::adapters::databricks::constants::{STAGE_FOLDER, STALE_STAGE_HOURS};
use crate::adapters::databricks::main::stage::_remove_folder::remove_folder;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::databricks_api::main::list_directory::list_directory;
use crate::databricks_api::models::DirectoryEntry;
use crate::http::errors::ApiError;
use crate::time_text::main::now_ms::now_ms;

/// Remove the load folders in `<volume>/sqlload` whose files are all over a day old; how many went.
pub(in crate::adapters) async fn remove_stale(
    destination: &DatabricksDestination,
) -> Result<usize, ApiError> {
    let api = &destination.api;
    let root = format!("{}/{STAGE_FOLDER}", destination.volume);
    let Some(loads) = list_directory(api, &root).await? else {
        return Ok(0);
    };
    let mut listed: Vec<(DirectoryEntry, Vec<DirectoryEntry>)> = Vec::new();
    for load in loads.into_iter().filter(|load| load.is_directory) {
        let mut files: Vec<DirectoryEntry> = Vec::new();
        for batch in list_directory(api, &load.path).await?.unwrap_or_default() {
            match batch.is_directory {
                true => files.extend(list_directory(api, &batch.path).await?.unwrap_or_default()),
                false => files.push(batch),
            }
        }
        listed.push((load, files));
    }
    let before_ms = now_ms() - STALE_STAGE_HOURS * 3_600_000;
    let stale = stale_folders(&listed, before_ms);
    for folder in &stale {
        remove_folder(api, folder).await?;
    }
    Ok(stale.len())
}
