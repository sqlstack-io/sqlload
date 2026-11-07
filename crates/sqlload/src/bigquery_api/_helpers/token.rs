//! Service-account keys and the signed assertions Google exchanges for access tokens.

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;

use crate::bigquery_api::constants::{ASSERTION_LIFETIME_SECS, SCOPE};
use crate::bigquery_api::models::ServiceAccountKey;
use crate::http::errors::ApiError;

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

/// An RS256 assertion that `key`'s service account asks for a BigQuery token, valid for an hour from `now` (seconds since the epoch).
pub(crate) fn signed_assertion(key: &ServiceAccountKey, now: u64) -> Result<String, ApiError> {
    let encoding_key = EncodingKey::from_rsa_pem(key.private_key.expose().as_bytes())
        .map_err(|error| ApiError::Auth(format!("cannot use the service-account key: {error}")))?;
    let claims = Claims {
        iss: &key.client_email,
        scope: SCOPE,
        aud: &key.token_uri,
        iat: now,
        exp: now + ASSERTION_LIFETIME_SECS,
    };
    jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .map_err(|error| ApiError::Auth(format!("cannot sign the token request: {error}")))
}
