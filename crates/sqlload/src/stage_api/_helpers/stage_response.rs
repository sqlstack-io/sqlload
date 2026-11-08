use serde_json::Value;

use crate::auth::models::Secret;
use crate::stage_api::constants::{NON_S3_STAGE, S3_LOCATION, SUPPORTED_CIPHER};
use crate::stage_api::models::{
    AwsCredentials, EncryptionMaterial, ObjectTarget, StageDownloads, StageFile, StageInfo,
};

fn text(value: &Value, name: &str) -> Result<String, String> {
    value[name]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("the stage response has no {name}"))
}

/// Read the S3 location, credentials and encryption material from a `PUT` response's `data`.
pub(crate) fn parse_stage_info(data: &Value) -> Result<StageInfo, String> {
    let stage = &data["stageInfo"];
    let location_type = stage["locationType"].as_str().unwrap_or_default();
    if location_type != S3_LOCATION {
        return Err(format!("{NON_S3_STAGE}, got {location_type:?}"));
    }
    let location = text(stage, "location")?;
    let (bucket, path) = location.split_once('/').unwrap_or((location.as_str(), ""));
    let mut prefix = path.to_owned();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    let region = text(stage, "region")?;
    let named = stage["endPoint"].as_str().filter(|value| !value.is_empty());
    let endpoint = match named {
        Some(endpoint) if endpoint.contains("://") => endpoint.to_owned(),
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
            path_style: named.is_some_and(|endpoint| endpoint.contains("://")),
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
        encryption: encryption_material(&material)?,
    })
}

/// The files a `GET` response names, each with its own encryption material, at the stage location it describes.
pub(crate) fn parse_stage_downloads(data: &Value) -> Result<StageDownloads, String> {
    let stage = parse_stage_info(data)?;
    let keys: Vec<&str> = data["src_locations"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let materials: Vec<Value> = match &data["encryptionMaterial"] {
        Value::Array(items) => items.clone(),
        other => vec![other.clone(); keys.len()],
    };
    if materials.len() != keys.len() {
        return Err(format!(
            "the GET response names {} files but has {} keys",
            keys.len(),
            materials.len()
        ));
    }
    let files = keys
        .into_iter()
        .zip(&materials)
        .map(|(key, material)| {
            Ok(StageFile {
                key: key.to_owned(),
                encryption: encryption_material(material)?,
            })
        })
        .collect::<Result<Vec<StageFile>, String>>()?;
    Ok(StageDownloads {
        target: stage.target,
        credentials_expire_ms: stage.credentials_expire_ms,
        files,
    })
}

fn encryption_material(material: &Value) -> Result<EncryptionMaterial, String> {
    Ok(EncryptionMaterial {
        query_stage_master_key: Secret(text(material, "queryStageMasterKey")?),
        query_id: text(material, "queryId")?,
        smk_id: smk_id(&material["smkId"]).ok_or("the stage response has no smkId")?,
    })
}

fn smk_id(value: &Value) -> Option<i64> {
    if let Some(number) = value.as_i64() {
        return Some(number);
    }
    match value.as_str()?.parse::<i64>() {
        Ok(number) => Some(number),
        Err(_) => None,
    }
}
