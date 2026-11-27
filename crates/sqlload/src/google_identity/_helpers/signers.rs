//! The first token of a sign-in: from a service-account key, a gcloud login, the metadata server, or workload identity federation.

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;

use crate::auth::models::Secret;
use crate::google_identity::_helpers::exchange::{access_token, answer};
use crate::google_identity::_helpers::federation::federated_token;
use crate::google_identity::constants::{
    ASSERTION_LIFETIME_SECS, JWT_BEARER_GRANT, METADATA_FLAVOR, METADATA_TIMEOUT,
};
use crate::google_identity::models::{AuthorizedUser, ServiceAccountKey, Signer};
use crate::http::errors::ApiError;

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

/// A token with `scope` from `signer`, and when it expires (milliseconds since the epoch, `now_ms` being now). A gcloud login's token has the scopes its login gave, and the metadata server's those of the machine.
pub(crate) async fn signer_token(
    http: &reqwest::Client,
    signer: &Signer,
    scope: &str,
    now_ms: i64,
) -> Result<(Secret, i64), ApiError> {
    match signer {
        Signer::ServiceAccount(key) => key_token(http, key, scope, now_ms).await,
        Signer::User(user) => user_token(http, user, now_ms).await,
        Signer::Metadata { host } => metadata_token(http, host, scope, now_ms).await,
        Signer::External(account) => federated_token(http, account, now_ms).await,
    }
}

/// A token for `key`'s service account: a signed assertion, exchanged at its `token_uri`.
async fn key_token(
    http: &reqwest::Client,
    key: &ServiceAccountKey,
    scope: &str,
    now_ms: i64,
) -> Result<(Secret, i64), ApiError> {
    let step = format!(
        "Google's token endpoint, for the service-account key of {}",
        key.client_email
    );
    let assertion = signed_assertion(key, scope, u64::try_from(now_ms / 1000).unwrap_or_default())?;
    let request = http
        .post(&key.token_uri)
        .form(&[("grant_type", JWT_BEARER_GRANT), ("assertion", &assertion)]);
    access_token(
        &answer(request, &step).await?,
        ("access_token", &step),
        now_ms,
    )
}

/// An RS256 assertion that `key`'s service account asks for a token with `scope`, valid for an hour from `now` (seconds since the epoch).
pub(crate) fn signed_assertion(
    key: &ServiceAccountKey,
    scope: &str,
    now: u64,
) -> Result<String, ApiError> {
    let encoding_key = EncodingKey::from_rsa_pem(key.private_key.expose().as_bytes())
        .map_err(|error| ApiError::Auth(format!("cannot use the service-account key: {error}")))?;
    let claims = Claims {
        iss: &key.client_email,
        scope,
        aud: &key.token_uri,
        iat: now,
        exp: now + ASSERTION_LIFETIME_SECS,
    };
    jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .map_err(|error| ApiError::Auth(format!("cannot sign the token request: {error}")))
}

/// A gcloud login's token, renewed with its refresh token.
async fn user_token(
    http: &reqwest::Client,
    user: &AuthorizedUser,
    now_ms: i64,
) -> Result<(Secret, i64), ApiError> {
    let step = "renewing the gcloud login's token (run `gcloud auth application-default login` again if Google refuses it)";
    let request = http.post(&user.token_uri).form(&[
        ("grant_type", "refresh_token"),
        ("refresh_token", user.refresh_token.expose()),
        ("client_id", &user.client_id),
        ("client_secret", user.client_secret.expose()),
    ]);
    access_token(
        &answer(request, step).await?,
        ("access_token", step),
        now_ms,
    )
}

/// The attached service account's token, from the metadata server at `host`, asking for `scope` (Compute Engine gives the scopes the machine was created with).
async fn metadata_token(
    http: &reqwest::Client,
    host: &str,
    scope: &str,
    now_ms: i64,
) -> Result<(Secret, i64), ApiError> {
    let step = format!(
        "the metadata server ({host}), for the machine's service account; off Google Cloud, sign in with service_account_key, GOOGLE_APPLICATION_CREDENTIALS or `gcloud auth application-default login`"
    );
    let request = http
        .get(format!(
            "http://{host}/computeMetadata/v1/instance/service-accounts/default/token"
        ))
        .query(&[("scopes", scope)])
        .header(METADATA_FLAVOR.0, METADATA_FLAVOR.1)
        .timeout(METADATA_TIMEOUT);
    access_token(
        &answer(request, &step).await?,
        ("access_token", &step),
        now_ms,
    )
}
