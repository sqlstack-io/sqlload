//! Append rows to a channel.

use crate::http::errors::ApiError;
use crate::streaming_api::_helpers::requests::{pipe_path, send};
use crate::streaming_api::models::{AppendRequest, AppendResponse, StreamingClient};

/// Append one gzip NDJSON body; returns the next continuation token.
pub async fn append_rows(
    client: &StreamingClient,
    append: AppendRequest<'_>,
) -> Result<String, ApiError> {
    let path = format!(
        "/v2/streaming/data/{}/channels/{}/rows",
        pipe_path(client, append.pipe),
        append.channel
    );
    let offset_token = append.offset.to_string();
    let response = send(client, reqwest::Method::POST, &path, |request| {
        request
            .query(&[
                ("continuationToken", append.continuation),
                ("offsetToken", offset_token.as_str()),
            ])
            .header("Content-Type", "application/x-ndjson")
            .header("Content-Encoding", "gzip")
            .body(append.body.clone())
    })
    .await?;
    let appended: AppendResponse = response.json().await?;
    Ok(appended.next_continuation_token)
}
