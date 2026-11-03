use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// A loaded private key and the public key fingerprint Snowflake expects in `iss`.
pub(crate) struct KeyPair {
    pub(crate) encoding_key: EncodingKey,
    pub(crate) fingerprint: String,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: String,
    sub: &'a str,
    iat: u64,
    exp: u64,
}

/// Load an unencrypted PKCS#8 or PKCS#1 PEM private key.
pub(crate) fn load_key_pair(pem: &str) -> Result<KeyPair, String> {
    let private = RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .map_err(|error| {
            format!("cannot read the private key (encrypted keys are not supported): {error}")
        })?;
    let der = private
        .to_public_key()
        .to_public_key_der()
        .map_err(|error| format!("cannot derive the public key: {error}"))?;
    let fingerprint = format!("SHA256:{}", STANDARD.encode(Sha256::digest(der.as_bytes())));
    let encoding_key = EncodingKey::from_rsa_pem(pem.as_bytes())
        .map_err(|error| format!("cannot use the private key: {error}"))?;
    Ok(KeyPair {
        encoding_key,
        fingerprint,
    })
}

/// A key-pair JWT for `ACCOUNT.USER`, valid from `now` for `lifetime_secs`.
pub(crate) fn sign_jwt(
    key: &KeyPair,
    qualified_user: &str,
    now: u64,
    lifetime_secs: u64,
) -> Result<String, String> {
    let claims = Claims {
        iss: format!("{qualified_user}.{}", key.fingerprint),
        sub: qualified_user,
        iat: now,
        exp: now + lifetime_secs,
    };
    jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &key.encoding_key)
        .map_err(|error| format!("cannot sign the JWT: {error}"))
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KeyPair(<redacted>)")
    }
}
