//! List a volume directory.

use serde_json::Value;

use crate::databricks_api::_helpers::requests::send;
use crate::databricks_api::constants::{DIRECTORIES_PATH, NOT_FOUND};
use crate::databricks_api::models::{DatabricksClient, DirectoryEntry};
use crate::http::errors::ApiError;

/// The entries directly in `path` (`/Volumes/...`), every page; `None` when the directory does not exist.
pub async fn list_directory(
    client: &DatabricksClient,
    path: &str,
) -> Result<Option<Vec<DirectoryEntry>>, ApiError> {
    let url = format!("{}{DIRECTORIES_PATH}{path}", client.host);
    let mut entries: Vec<DirectoryEntry> = Vec::new();
    let mut page: Option<String> = None;
    loop {
        let sent = send(client, |http| match &page {
            Some(token) => http.get(&url).query(&[("page_token", token)]),
            None => http.get(&url),
        })
        .await;
        let body: Value = match sent {
            Err(error) if error.status() == Some(NOT_FOUND) => return Ok(None),
            other => other?.json().await?,
        };
        entries.extend(
            body["contents"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|entry| DirectoryEntry {
                    path: entry["path"].as_str().unwrap_or_default().to_owned(),
                    is_directory: entry["is_directory"].as_bool().unwrap_or(false),
                    last_modified_ms: entry["last_modified"].as_i64(),
                }),
        );
        match body["next_page_token"].as_str() {
            Some(token) if !token.is_empty() => page = Some(token.to_owned()),
            _ => return Ok(Some(entries)),
        }
    }
}
