//! Upload one object with SigV4.

use crate::http::errors::ApiError;
use crate::stage_api::_helpers::s3_errors::outcome_from_failure;
use crate::stage_api::_helpers::sigv4::{SignInput, UNSIGNED_PAYLOAD, sign_v4, uri_encode};
use crate::stage_api::constants::HOST_HEADER;
use crate::stage_api::models::{ObjectUpload, PutOutcome};
use crate::time_text::main::amz_date::amz_date;
use crate::time_text::main::now_ms::now_ms;

/// Upload `body` to `target` under `key` with extra signed `headers`.
pub async fn put_object(
    http: &reqwest::Client,
    upload: ObjectUpload<'_>,
    body: Vec<u8>,
) -> Result<PutOutcome, ApiError> {
    let ObjectUpload {
        target,
        key,
        headers: extra_headers,
    } = upload;
    let (host, path) = target.host_and_path(key);
    let canonical_uri = uri_encode(&path, true);
    let now = amz_date(now_ms());
    let mut headers = extra_headers.clone();
    headers.insert(HOST_HEADER.to_owned(), host.clone());
    headers.insert(
        "x-amz-content-sha256".to_owned(),
        UNSIGNED_PAYLOAD.to_owned(),
    );
    headers.insert("x-amz-date".to_owned(), now.clone());
    if let Some(token) = &target.credentials.session_token {
        headers.insert("x-amz-security-token".to_owned(), token.expose().to_owned());
    }
    let authorization = sign_v4(&SignInput {
        method: "PUT",
        canonical_uri: &canonical_uri,
        canonical_query: "",
        headers: &headers,
        payload_hash: UNSIGNED_PAYLOAD,
        region: &target.region,
        amz_date: &now,
        key_id: &target.credentials.key_id,
        secret: target.credentials.secret.expose(),
    });
    let mut request = http
        .put(format!("{}://{host}{canonical_uri}", target.scheme()))
        .header("authorization", authorization)
        .header("content-type", "application/octet-stream")
        .body(body);
    for (name, value) in headers
        .iter()
        .filter(|(name, _)| name.as_str() != HOST_HEADER)
    {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request.send().await?;
    if response.status().is_success() {
        return Ok(PutOutcome::Uploaded);
    }
    outcome_from_failure(response).await
}
