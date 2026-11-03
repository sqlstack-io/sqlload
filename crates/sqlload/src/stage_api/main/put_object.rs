//! Upload one object with SigV4.

use crate::http::errors::ApiError;
use crate::stage_api::_helpers::s3_errors::outcome_from_failure;
use crate::stage_api::_helpers::signed_request::signed_request;
use crate::stage_api::models::{ObjectUpload, PutOutcome};

/// Upload `body` to `target` under `key` with extra signed `headers`.
pub async fn put_object(
    http: &reqwest::Client,
    upload: ObjectUpload<'_>,
    body: Vec<u8>,
) -> Result<PutOutcome, ApiError> {
    let response = signed_request(http, reqwest::Method::PUT, upload)
        .header("content-type", "application/octet-stream")
        .body(body)
        .send()
        .await?;
    if response.status().is_success() {
        return Ok(PutOutcome::Uploaded);
    }
    outcome_from_failure(response).await
}
