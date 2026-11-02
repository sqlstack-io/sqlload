//! Open named channels.

use serde_json::json;

use crate::http::errors::ApiError;
use crate::streaming_api::_helpers::requests::{pipe_path, send};
use crate::streaming_api::models::{OpenResponse, OpenedChannel, StreamingClient};

/// Open (or reopen) a named channel; reopening discards rows Snowflake has not committed.
pub async fn open_channel(
    client: &StreamingClient,
    pipe: &str,
    channel: &str,
) -> Result<OpenedChannel, ApiError> {
    let path = format!(
        "/v2/streaming/{}/channels/{channel}",
        pipe_path(client, pipe)
    );
    let body = json!({});
    let response = send(client, reqwest::Method::PUT, &path, |request| {
        request.json(&body)
    })
    .await?;
    let opened: OpenResponse = response.json().await?;
    Ok(OpenedChannel {
        continuation_token: opened.next_continuation_token,
        committed_offset: opened.channel_status.committed_offset(),
    })
}
