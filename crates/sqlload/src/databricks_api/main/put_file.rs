//! Upload a file to a volume.

use crate::databricks_api::_helpers::requests::send;
use crate::databricks_api::constants::FILES_PATH;
use crate::databricks_api::models::DatabricksClient;
use crate::http::errors::ApiError;

/// Write `body` to `path` (`/Volumes/...`), replacing a file already there.
pub async fn put_file(
    client: &DatabricksClient,
    path: &str,
    body: bytes::Bytes,
) -> Result<(), ApiError> {
    let url = format!("{}{FILES_PATH}{path}", client.host);
    send(client, |http| {
        http.put(&url)
            .query(&[("overwrite", "true")])
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body.clone())
    })
    .await?;
    Ok(())
}
