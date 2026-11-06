//! Check the staging volume.

use crate::adapters::databricks::models::DatabricksDestination;
use crate::databricks_api::main::list_directory::list_directory;
use crate::http::errors::ApiError;

/// What `debug` says about the staging volume: that it lists, or why not.
pub(in crate::adapters) async fn check_volume(
    destination: &DatabricksDestination,
) -> Result<String, ApiError> {
    match list_directory(&destination.api, &destination.volume).await? {
        Some(entries) => Ok(format!(
            "volume {} lists ({} entries at the top)",
            destination.volume,
            entries.len()
        )),
        None => Err(ApiError::Protocol(format!(
            "volume {} does not exist or cannot be read; create it with CREATE VOLUME and grant READ VOLUME and WRITE VOLUME",
            destination.volume
        ))),
    }
}
