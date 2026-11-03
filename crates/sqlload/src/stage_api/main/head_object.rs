//! Check whether one object exists, with SigV4.

use std::collections::BTreeMap;

use crate::http::errors::ApiError;
use crate::stage_api::_helpers::signed_request::signed_request;
use crate::stage_api::constants::{BAD_REQUEST, NOT_FOUND};
use crate::stage_api::models::{ObjectPresence, ObjectTarget, ObjectUpload};

/// Whether `key` exists under `target`; S3 answers a `HEAD` with expired credentials with a bodiless 400.
pub async fn head_object(
    http: &reqwest::Client,
    target: &ObjectTarget,
    key: &str,
) -> Result<ObjectPresence, ApiError> {
    let headers: BTreeMap<String, String> = BTreeMap::new();
    let object = ObjectUpload {
        target,
        key,
        headers: &headers,
    };
    let response = signed_request(http, reqwest::Method::HEAD, object)
        .send()
        .await?;
    let status = response.status().as_u16();
    match status {
        _ if response.status().is_success() => Ok(ObjectPresence::Found),
        NOT_FOUND => Ok(ObjectPresence::Missing),
        BAD_REQUEST => Ok(ObjectPresence::CredentialsExpired),
        _ => Err(ApiError::Status {
            status,
            code: String::new(),
            message: "S3 HEAD failed".to_owned(),
        }),
    }
}
