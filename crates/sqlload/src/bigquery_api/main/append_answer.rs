//! Read the next answer of an `AppendRows` call.

use crate::bigquery_api::_helpers::grpc::status_error;
use crate::bigquery_api::models::AppendRowsResponse;
use crate::http::errors::ApiError;

/// The next answer on `responses`, `None` once the call has ended cleanly; a call the server ended with an error gives that error.
pub async fn append_answer(
    responses: &mut tonic::Streaming<AppendRowsResponse>,
) -> Result<Option<AppendRowsResponse>, ApiError> {
    responses
        .message()
        .await
        .map_err(|status| status_error(&status))
}
