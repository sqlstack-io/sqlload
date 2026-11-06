//! Delete a file or an empty directory in a volume.

use crate::databricks_api::_helpers::requests::send;
use crate::databricks_api::constants::{DIRECTORIES_PATH, FILES_PATH, NOT_FOUND};
use crate::databricks_api::models::DatabricksClient;
use crate::http::errors::ApiError;

/// Delete the file at `path`, or the directory when `directory` (which must be empty); a path already gone is not an error.
pub async fn delete_path(
    client: &DatabricksClient,
    path: &str,
    directory: bool,
) -> Result<(), ApiError> {
    let api = match directory {
        true => DIRECTORIES_PATH,
        false => FILES_PATH,
    };
    let url = format!("{}{api}{path}", client.host);
    match send(client, |http| http.delete(&url)).await {
        Err(error) if error.status() == Some(NOT_FOUND) => Ok(()),
        other => other.map(|_| ()),
    }
}
