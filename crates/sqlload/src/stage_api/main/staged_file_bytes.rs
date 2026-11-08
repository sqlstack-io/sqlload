//! Download and decrypt one internal-stage file.

use std::collections::BTreeMap;

use crate::http::errors::ApiError;
use crate::stage_api::_helpers::encryption::decrypt;
use crate::stage_api::_helpers::signed_request::signed_request;
use crate::stage_api::constants::{IV_HEADER, KEY_HEADER, MATDESC_HEADER};
use crate::stage_api::models::{EncryptionMetadata, ObjectTarget, ObjectUpload, StageFile};

/// The plaintext of `file` under `target`: its bytes from S3, decrypted with the file key wrapped in its metadata.
pub async fn staged_file_bytes(
    http: &reqwest::Client,
    target: &ObjectTarget,
    file: &StageFile,
) -> Result<Vec<u8>, ApiError> {
    let headers: BTreeMap<String, String> = BTreeMap::new();
    let object = ObjectUpload {
        target,
        key: &file.key,
        headers: &headers,
    };
    let response = signed_request(http, reqwest::Method::GET, object)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(ApiError::Status {
            status: status.as_u16(),
            code: String::new(),
            message: "S3 GET failed".to_owned(),
        });
    }
    let header =
        |name: &str| -> Result<String, ApiError> {
            let value = response.headers().get(name).ok_or_else(|| {
                ApiError::Protocol(format!("the stage file has no {name} header"))
            })?;
            value.to_str().map(str::to_owned).map_err(|error| {
                ApiError::Protocol(format!("the stage file's {name} header: {error}"))
            })
        };
    let metadata = EncryptionMetadata {
        key: header(KEY_HEADER)?,
        iv: header(IV_HEADER)?,
        matdesc: match response.headers().contains_key(MATDESC_HEADER) {
            true => header(MATDESC_HEADER)?,
            false => String::new(),
        },
    };
    let body = response.bytes().await?;
    decrypt(&file.encryption, &metadata, &body).map_err(ApiError::Protocol)
}
