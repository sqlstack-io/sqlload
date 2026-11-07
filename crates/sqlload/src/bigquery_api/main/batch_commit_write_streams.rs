//! Commit finalized pending streams at once.

use crate::bigquery_api::_helpers::grpc::unary;
use crate::bigquery_api::constants::BATCH_COMMIT_WRITE_STREAMS;
use crate::bigquery_api::models::{
    BatchCommitWriteStreamsRequest, BatchCommitWriteStreamsResponse, BigQueryClient, Timestamp,
};
use crate::http::errors::ApiError;

/// Commit the finalized `streams` of the table `parent` in one atomic step, so their rows appear together; when any stream cannot commit, none does, and the error names each one and why.
pub async fn batch_commit_write_streams(
    client: &BigQueryClient,
    parent: &str,
    streams: &[String],
) -> Result<Timestamp, ApiError> {
    let response: BatchCommitWriteStreamsResponse = unary(
        client,
        BATCH_COMMIT_WRITE_STREAMS,
        &format!("parent={parent}"),
        BatchCommitWriteStreamsRequest {
            parent: parent.to_owned(),
            write_streams: streams.to_vec(),
        },
    )
    .await?;
    if !response.stream_errors.is_empty() {
        let reasons: Vec<String> = response
            .stream_errors
            .iter()
            .map(|error| {
                format!(
                    "{}: {} (code {})",
                    error.entity, error.error_message, error.code
                )
            })
            .collect();
        return Err(ApiError::Status {
            status: 400,
            code: "streamErrors".to_owned(),
            message: format!(
                "BigQuery committed none of the streams: {}",
                reasons.join("; ")
            ),
        });
    }
    response
        .commit_time
        .ok_or_else(|| ApiError::Protocol("the commit answer has no commit time".to_owned()))
}
