//! Read channel status.

use std::collections::BTreeMap;

use serde_json::json;

use crate::http::errors::ApiError;
use crate::streaming_api::_helpers::requests::{pipe_path, send};
use crate::streaming_api::models::{BulkStatusResponse, ChannelStatus, StreamingClient};

/// Status of `channels` on `pipe`, keyed by upper-case channel name.
pub async fn channel_statuses(
    client: &StreamingClient,
    pipe: &str,
    channels: &[String],
) -> Result<BTreeMap<String, ChannelStatus>, ApiError> {
    let path = format!(
        "/v2/streaming/{}:bulk-channel-status",
        pipe_path(client, pipe)
    );
    let body = json!({ "channel_names": channels });
    let response = send(client, reqwest::Method::POST, &path, |request| {
        request.json(&body)
    })
    .await?;
    let parsed: BulkStatusResponse = response.json().await?;
    Ok(parsed
        .channel_statuses
        .into_iter()
        .map(|(name, status)| (name.to_ascii_uppercase(), status))
        .collect())
}
