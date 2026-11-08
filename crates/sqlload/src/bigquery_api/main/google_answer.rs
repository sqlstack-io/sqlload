//! Google's answers, read the way every Google API writes its errors.

use crate::bigquery_api::_helpers::requests::google_response;
use crate::http::errors::ApiError;

/// `response`, or an [`ApiError`] with the reason and message of Google's error body (a refusal for a rate limit counts as throttling).
pub async fn google_answer(response: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    google_response(response).await
}
