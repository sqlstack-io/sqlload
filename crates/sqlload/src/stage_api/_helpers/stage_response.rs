use serde_json::Value;

use crate::auth::models::Secret;
use crate::stage_api::constants::{S3_LOCATION, SUPPORTED_CIPHER};
use crate::stage_api::models::{AwsCredentials, EncryptionMaterial, ObjectTarget, StageInfo};

fn text(value: &Value, name: &str) -> Result<String, String> {
    value[name]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("PUT response has no {name}"))
}

/// Read the S3 location, credentials and encryption material from a `PUT` response's `data`.
pub(crate) fn parse_stage_info(data: &Value) -> Result<StageInfo, String> {
    let stage = &data["stageInfo"];
    let location_type = stage["locationType"].as_str().unwrap_or_default();
    if location_type != S3_LOCATION {
        return Err(format!(
            "only S3-backed internal stages are supported, got {location_type:?}"
        ));
    }
    let location = text(stage, "location")?;
    let (bucket, path) = location.split_once('/').unwrap_or((location.as_str(), ""));
    let mut prefix = path.to_owned();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    let region = text(stage, "region")?;
    let endpoint = match stage["endPoint"].as_str().filter(|value| !value.is_empty()) {
        Some(endpoint) => format!("https://{endpoint}"),
        None => format!("https://s3.{region}.amazonaws.com"),
    };
    let credentials = &stage["creds"];
    let material = match &data["encryptionMaterial"] {
        Value::Array(items) => items.first().cloned().unwrap_or(Value::Null),
        other => other.clone(),
    };
    if material.is_null() {
        return Err(
            "the stage is not client-side encrypted; create it with the default encryption"
                .to_owned(),
        );
    }
    let cipher = stage["ciphers"].as_str().unwrap_or(SUPPORTED_CIPHER);
    if cipher != SUPPORTED_CIPHER {
        return Err(format!(
            "unsupported stage cipher {cipher:?}; only AES_CBC is implemented"
        ));
    }
    Ok(StageInfo {
        target: ObjectTarget {
            endpoint,
            bucket: bucket.to_owned(),
            path_style: false,
            prefix,
            region,
            credentials: AwsCredentials {
                key_id: text(credentials, "AWS_KEY_ID")?,
                secret: Secret(text(credentials, "AWS_SECRET_KEY")?),
                session_token: credentials["AWS_TOKEN"]
                    .as_str()
                    .map(|token| Secret(token.to_owned())),
            },
        },
        credentials_expire_ms: stage["credExpiryTime"].as_i64(),
        encryption: EncryptionMaterial {
            query_stage_master_key: Secret(text(&material, "queryStageMasterKey")?),
            query_id: text(&material, "queryId")?,
            smk_id: material["smkId"]
                .as_i64()
                .or_else(|| {
                    material["smkId"]
                        .as_str()
                        .and_then(|value| value.parse().ok())
                })
                .ok_or("PUT response has no smkId")?,
        },
    })
}
