//! Tokens for a service account another identity may act as, from IAM Credentials `generateAccessToken`.

use serde_json::json;

use crate::auth::models::Secret;
use crate::google_identity::_helpers::exchange::{access_token, answer};
use crate::google_identity::models::Impersonation;
use crate::http::errors::ApiError;

/// A token with `scope` for `target`'s service account, asked for with the signer's token `source`; the signer needs `roles/iam.serviceAccountTokenCreator` on that account (or on each delegate in turn).
pub(crate) async fn impersonated_token(
    http: &reqwest::Client,
    target: &Impersonation,
    (source, scope): (&Secret, &str),
    now_ms: i64,
) -> Result<(Secret, i64), ApiError> {
    let step = format!(
        "IAM Credentials' generateAccessToken, impersonating {} (the identity signing in needs roles/iam.serviceAccountTokenCreator on it)",
        target.account
    );
    let delegates: Vec<String> = target
        .delegates
        .iter()
        .map(|account| format!("projects/-/serviceAccounts/{account}"))
        .collect();
    let request = http
        .post(&target.url)
        .bearer_auth(source.expose())
        .json(&json!({
            "scope": [scope],
            "delegates": delegates,
            "lifetime": format!("{}s", target.lifetime_secs),
        }));
    access_token(
        &answer(request, &step).await?,
        ("accessToken", &step),
        now_ms,
    )
}
