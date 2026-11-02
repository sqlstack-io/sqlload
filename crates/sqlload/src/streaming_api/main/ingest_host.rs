//! Discover the ingest host.

use crate::http::errors::ApiError;
use crate::streaming_api::_helpers::requests::access;
use crate::streaming_api::models::StreamingClient;

/// The ingest base URL for this account.
pub async fn ingest_host(client: &StreamingClient) -> Result<String, ApiError> {
    Ok(access(client).await?.base)
}
