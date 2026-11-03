//! Map error responses.

use crate::http::_helpers::error_body::error_fields;
use crate::http::errors::ApiError;

/// Turn a non-success response into an [`ApiError`] carrying the service code and message.
pub async fn check_response(response: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    let (code, message) = error_fields(&body);
    Err(ApiError::Status {
        status,
        code,
        message,
    })
}
