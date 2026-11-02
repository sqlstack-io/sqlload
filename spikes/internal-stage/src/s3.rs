//! Minimal S3 PUT with hand-rolled SigV4 (virtual-hosted style, unsigned payload),
//! matching what the official drivers send for internal-stage uploads.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::encrypt::EncryptionMetadata;
use crate::snowflake::StageInfo;
use crate::util::{amz_date, unix_now};

type HmacSha256 = Hmac<Sha256>;

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

pub enum PutOutcome {
    Uploaded,
    /// The stage credentials expired; refresh them and retry.
    ExpiredToken,
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    m.update(data.as_bytes());
    m.finalize().into_bytes().to_vec()
}

/// RFC 3986 encoding as SigV4 expects; `/` kept when encoding a path.
pub fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub struct SignInput<'a> {
    pub method: &'a str,
    /// Already URI-encoded path, starting with `/`.
    pub canonical_uri: &'a str,
    pub canonical_query: &'a str,
    /// Lowercase header names; all of them are signed.
    pub headers: &'a BTreeMap<String, String>,
    pub payload_hash: &'a str,
    pub region: &'a str,
    pub amz_date: &'a str,
    pub key_id: &'a str,
    pub secret: &'a str,
}

/// Returns the `Authorization` header value.
pub fn sign_v4(i: &SignInput<'_>) -> String {
    let canonical_headers: String = i
        .headers
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed_headers = i.headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        i.method,
        i.canonical_uri,
        i.canonical_query,
        canonical_headers,
        signed_headers,
        i.payload_hash
    );
    let date = &i.amz_date[..8];
    let scope = format!("{date}/{}/s3/aws4_request", i.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        i.amz_date,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac(format!("AWS4{}", i.secret).as_bytes(), date);
    let k_region = hmac(&k_date, i.region);
    let k_service = hmac(&k_region, "s3");
    let k_signing = hmac(&k_service, "aws4_request");
    let signature = hex::encode(hmac(&k_signing, &string_to_sign));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        i.key_id
    )
}

/// Uploads one encrypted object to `<stage location>/<key>`.
pub async fn put_object(
    client: &reqwest::Client,
    stage: &StageInfo,
    key: &str,
    body: Vec<u8>,
    sfc_digest: &str,
    enc: &EncryptionMetadata,
) -> Result<PutOutcome> {
    let host = stage.s3_host();
    let object_path = format!("{}{}", stage.path_prefix, key.trim_start_matches('/'));
    let canonical_uri = format!("/{}", uri_encode(&object_path, true));

    let mut headers = BTreeMap::new();
    headers.insert("host".to_string(), host.clone());
    headers.insert(
        "x-amz-content-sha256".to_string(),
        UNSIGNED_PAYLOAD.to_string(),
    );
    let now = amz_date(unix_now());
    headers.insert("x-amz-date".to_string(), now.clone());
    headers.insert(
        "x-amz-security-token".to_string(),
        stage.creds.token.clone(),
    );
    headers.insert("x-amz-meta-sfc-digest".to_string(), sfc_digest.to_string());
    headers.insert("x-amz-meta-x-amz-iv".to_string(), enc.iv.clone());
    headers.insert("x-amz-meta-x-amz-key".to_string(), enc.key.clone());
    headers.insert("x-amz-meta-x-amz-matdesc".to_string(), enc.matdesc.clone());

    let auth = sign_v4(&SignInput {
        method: "PUT",
        canonical_uri: &canonical_uri,
        canonical_query: "",
        headers: &headers,
        payload_hash: UNSIGNED_PAYLOAD,
        region: &stage.region,
        amz_date: &now,
        key_id: &stage.creds.key_id,
        secret: &stage.creds.secret,
    });

    let mut req = client
        .put(format!("https://{host}{canonical_uri}"))
        .header("authorization", auth)
        .header("content-type", "application/octet-stream")
        .body(body);
    for (k, v) in &headers {
        if k != "host" {
            req = req.header(k.as_str(), v.as_str());
        }
    }
    let resp = req.send().await?;
    let status = resp.status();
    if status.is_success() {
        return Ok(PutOutcome::Uploaded);
    }
    let text = resp.text().await.unwrap_or_default();
    if text.contains("<Code>ExpiredToken</Code>") {
        return Ok(PutOutcome::ExpiredToken);
    }
    let code = text
        .split("<Code>")
        .nth(1)
        .and_then(|s| s.split("</Code>").next())
        .unwrap_or("unknown");
    bail!("S3 PUT failed: HTTP {status}, code {code}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_aws_documented_get_example() {
        // "Example: GET Object" from the AWS S3 SigV4 header-auth documentation.
        let mut headers = BTreeMap::new();
        headers.insert("host".into(), "examplebucket.s3.amazonaws.com".into());
        headers.insert("range".into(), "bytes=0-9".into());
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        headers.insert("x-amz-content-sha256".into(), empty.into());
        headers.insert("x-amz-date".into(), "20130524T000000Z".into());
        let auth = sign_v4(&SignInput {
            method: "GET",
            canonical_uri: "/test.txt",
            canonical_query: "",
            headers: &headers,
            payload_hash: empty,
            region: "us-east-1",
            amz_date: "20130524T000000Z",
            key_id: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        });
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn encodes_paths() {
        assert_eq!(uri_encode("a b/c$d.gz", true), "a%20b/c%24d.gz");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
    }
}
