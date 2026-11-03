use crate::stage_api::_helpers::sigv4::{SignInput, UNSIGNED_PAYLOAD, sign_v4, uri_encode};
use crate::stage_api::constants::HOST_HEADER;
use crate::stage_api::models::ObjectUpload;
use crate::time_text::main::amz_date::amz_date;
use crate::time_text::main::now_ms::now_ms;

/// A SigV4-signed `method` request for the object, with its extra signed headers.
pub(crate) fn signed_request(
    http: &reqwest::Client,
    method: reqwest::Method,
    object: ObjectUpload<'_>,
) -> reqwest::RequestBuilder {
    let ObjectUpload {
        target,
        key,
        headers: extra_headers,
    } = object;
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
        method: method.as_str(),
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
        .request(
            method,
            format!("{}://{host}{canonical_uri}", target.scheme()),
        )
        .header("authorization", authorization);
    for (name, value) in headers
        .iter()
        .filter(|(name, _)| name.as_str() != HOST_HEADER)
    {
        request = request.header(name.as_str(), value.as_str());
    }
    request
}
