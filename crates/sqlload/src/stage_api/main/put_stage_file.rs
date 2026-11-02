//! Upload an encrypted file to an internal stage.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use sha2::{Digest, Sha256};

use crate::http::errors::ApiError;
use crate::stage_api::_helpers::encryption::encrypt;
use crate::stage_api::main::put_object::put_object;
use crate::stage_api::models::{PutOutcome, StageInfo};

/// Encrypt a gzip file with the stage key and upload it to the internal stage under `key`.
pub async fn put_stage_file(
    http: &reqwest::Client,
    stage: &StageInfo,
    key: &str,
    gzip_body: &[u8],
) -> Result<PutOutcome, ApiError> {
    let digest = STANDARD.encode(Sha256::digest(gzip_body));
    let (ciphertext, metadata) =
        encrypt(&stage.encryption, gzip_body).map_err(ApiError::Protocol)?;
    let headers = BTreeMap::from([
        ("x-amz-meta-sfc-digest".to_owned(), digest),
        ("x-amz-meta-x-amz-iv".to_owned(), metadata.iv),
        ("x-amz-meta-x-amz-key".to_owned(), metadata.key),
        ("x-amz-meta-x-amz-matdesc".to_owned(), metadata.matdesc),
    ]);
    put_object(http, &stage.target, key, ciphertext, headers).await
}
