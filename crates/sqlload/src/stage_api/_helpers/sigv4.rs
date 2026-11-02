use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub(crate) const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// What a SigV4 signature covers.
pub(crate) struct SignInput<'a> {
    pub(crate) method: &'a str,
    pub(crate) canonical_uri: &'a str,
    pub(crate) canonical_query: &'a str,
    pub(crate) headers: &'a BTreeMap<String, String>,
    pub(crate) payload_hash: &'a str,
    pub(crate) region: &'a str,
    pub(crate) amz_date: &'a str,
    pub(crate) key_id: &'a str,
    pub(crate) secret: &'a str,
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    match Hmac::<Sha256>::new_from_slice(key) {
        Ok(mut mac) => {
            mac.update(data.as_bytes());
            mac.finalize().into_bytes().to_vec()
        }
        Err(_) => Vec::new(),
    }
}

/// RFC 3986 encoding as SigV4 expects; `/` is kept when `keep_slash`.
pub(crate) fn uri_encode(text: &str, keep_slash: bool) -> String {
    let mut output = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                output.push(char::from(byte))
            }
            b'/' if keep_slash => output.push('/'),
            _ => output.push_str(&format!("%{byte:02X}")),
        }
    }
    output
}

/// The `Authorization` header for an S3 request; every header in `headers` is signed.
pub(crate) fn sign_v4(input: &SignInput<'_>) -> String {
    let canonical_headers: String = input
        .headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();
    let signed_headers = input.headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        input.method, input.canonical_uri, input.canonical_query, input.payload_hash
    );
    let date = &input.amz_date[..8];
    let scope = format!("{date}/{}/s3/aws4_request", input.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        input.amz_date,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let date_key = hmac(format!("AWS4{}", input.secret).as_bytes(), date);
    let region_key = hmac(&date_key, input.region);
    let service_key = hmac(&region_key, "s3");
    let signing_key = hmac(&service_key, "aws4_request");
    let signature = hex::encode(hmac(&signing_key, &string_to_sign));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        input.key_id
    )
}
