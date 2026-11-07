//! Finalize a write stream.

use crate::bigquery_api::_helpers::grpc::unary;
use crate::bigquery_api::constants::FINALIZE_WRITE_STREAM;
use crate::bigquery_api::models::{
    BigQueryClient, FinalizeWriteStreamRequest, FinalizeWriteStreamResponse,
};
use crate::http::errors::ApiError;

/// Close the stream `name` to further appends, ready for commit; the rows it holds.
pub async fn finalize_write_stream(client: &BigQueryClient, name: &str) -> Result<i64, ApiError> {
    let response: FinalizeWriteStreamResponse = unary(
        client,
        FINALIZE_WRITE_STREAM,
        &format!("name={name}"),
        FinalizeWriteStreamRequest {
            name: name.to_owned(),
        },
    )
    .await?;
    Ok(response.row_count)
}
