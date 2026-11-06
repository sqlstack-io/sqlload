//! Remove a folder from the staging volume.

use crate::databricks_api::main::delete_path::delete_path;
use crate::databricks_api::main::list_directory::list_directory;
use crate::databricks_api::models::DatabricksClient;
use crate::http::errors::ApiError;

/// Delete every file under `folder`, its folders, then `folder` itself; a folder already gone is not an error.
pub(in crate::adapters) async fn remove_folder(
    api: &DatabricksClient,
    folder: &str,
) -> Result<(), ApiError> {
    let mut pending: Vec<String> = vec![folder.to_owned()];
    let mut folders: Vec<String> = Vec::new();
    while let Some(next) = pending.pop() {
        for entry in list_directory(api, &next).await?.unwrap_or_default() {
            match entry.is_directory {
                true => pending.push(entry.path.trim_end_matches('/').to_owned()),
                false => delete_path(api, &entry.path, false).await?,
            }
        }
        folders.push(next);
    }
    for emptied in folders.iter().rev() {
        delete_path(api, emptied, true).await?;
    }
    Ok(())
}
