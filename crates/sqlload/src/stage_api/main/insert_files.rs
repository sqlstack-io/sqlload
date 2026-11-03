//! Register staged files with a pipe through Snowpipe REST.

use serde_json::{Value, json};

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::stage_api::_helpers::pipe_request::pipe_request;
use crate::stage_api::models::PipeTarget;

/// Ask `pipe` (fully qualified) to load `paths`, relative to its location; returns the paths it skipped because it received them before.
pub async fn insert_files(
    target: &PipeTarget<'_>,
    paths: &[String],
) -> Result<Vec<String>, ApiError> {
    let files: Vec<_> = paths.iter().map(|path| json!({ "path": path })).collect();
    let response = pipe_request(target, reqwest::Method::POST, "insertFiles")?
        .query(&[("showSkippedFiles", "true")])
        .json(&json!({ "files": files }))
        .send()
        .await?;
    let text = check_response(response).await?.text().await?;
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    let skipped = body["skippedFiles"]
        .as_array()
        .map(|paths| paths.iter().filter_map(skipped_path).collect())
        .unwrap_or_default();
    Ok(skipped)
}

/// A skipped file is listed as its path, or as an object with one.
fn skipped_path(entry: &Value) -> Option<String> {
    entry
        .as_str()
        .or_else(|| entry["path"].as_str())
        .map(str::to_owned)
}
