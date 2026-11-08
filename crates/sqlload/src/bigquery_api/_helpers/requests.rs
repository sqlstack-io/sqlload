//! Authorized requests, with the token fetched and kept until shortly before it expires, and Google's error bodies read.

use crate::auth::models::Secret;
use crate::bigquery_api::_helpers::token::signed_assertion;
use crate::bigquery_api::constants::{
    JWT_BEARER_GRANT, RATE_LIMITED, SCOPE, TOKEN_REFRESH_MARGIN_MS, UNAUTHORIZED,
};
use crate::bigquery_api::models::{BigQueryClient, ServiceAccountKey};
use crate::http::constants::TOO_MANY_REQUESTS;
use crate::http::errors::ApiError;
use crate::http::models::RetryPolicy;
use crate::time_text::main::now_ms::now_ms;

/// A token for the client's key: see [`token_for`].
pub(crate) async fn access_token(
    client: &BigQueryClient,
    refused: Option<&Secret>,
) -> Result<Secret, ApiError> {
    token_for(&client.http, &client.key, &client.token, (SCOPE, refused)).await
}

/// A token with `scope` for `key`: the one `kept`, or a new one when it is missing, near its expiry, or the one the server `refused`; a request refused with a token another request has already replaced gets the replacement.
pub(crate) async fn token_for(
    http: &reqwest::Client,
    key: &ServiceAccountKey,
    kept: &tokio::sync::Mutex<Option<(Secret, i64)>>,
    (scope, refused): (&str, Option<&Secret>),
) -> Result<Secret, ApiError> {
    let mut kept = kept.lock().await;
    if let Some((token, expires_ms)) = kept.as_ref()
        && refused.is_none_or(|refused| refused.expose() != token.expose())
        && now_ms() + TOKEN_REFRESH_MARGIN_MS < *expires_ms
    {
        return Ok(token.clone());
    }
    let now_secs = u64::try_from(now_ms() / 1000).unwrap_or_default();
    let assertion = signed_assertion(key, scope, now_secs)?;
    let sent = http
        .post(&key.token_uri)
        .form(&[("grant_type", JWT_BEARER_GRANT), ("assertion", &assertion)])
        .send()
        .await?;
    let response =
        google_response(sent)
            .await
            .map_err(|error| match (error.is_retryable(), error) {
                (
                    true,
                    ApiError::Status {
                        status,
                        code,
                        message,
                    },
                ) => ApiError::Status {
                    status,
                    code,
                    message: format!("from Google's token endpoint: {message}"),
                },
                (true, error) => error,
                (false, error) => ApiError::Auth(format!(
                    "Google refused the service-account key {}: {error}",
                    key.client_email
                )),
            })?;
    let body: serde_json::Value = response.json().await?;
    let token = body["access_token"]
        .as_str()
        .ok_or_else(|| ApiError::Protocol("the token response has no access_token".to_owned()))?;
    let lifetime_ms = body["expires_in"].as_i64().unwrap_or(3600) * 1000;
    let token = Secret(token.to_owned());
    *kept = Some((token.clone(), now_ms() + lifetime_ms));
    Ok(token)
}

/// `response`, or an [`ApiError`] with the reason and message of Google's `{"error": {...}}` body (an OAuth error's `error` and `error_description` too); a `rateLimitExceeded` refusal, which Google sends as 403, counts as throttling so it is retried.
pub(crate) async fn google_response(
    response: reqwest::Response,
) -> Result<reqwest::Response, ApiError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    let (code, message) = google_error(&body);
    let status = match code.as_str() {
        RATE_LIMITED => TOO_MANY_REQUESTS,
        _ => status,
    };
    Err(ApiError::Status {
        status,
        code,
        message,
    })
}

/// The reason and message of a Google error body, falling back to the start of the raw text.
pub(crate) fn google_error(body: &str) -> (String, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(parsed) => parsed,
        Err(_) => return (String::new(), body.chars().take(300).collect()),
    };
    let error = &parsed["error"];
    if let Some(text) = error.as_str() {
        let message = parsed["error_description"].as_str().unwrap_or(text);
        return (text.to_owned(), message.to_owned());
    }
    let reason = error["errors"][0]["reason"]
        .as_str()
        .or_else(|| error["status"].as_str())
        .unwrap_or_default();
    let message = error["message"]
        .as_str()
        .map_or_else(|| body.chars().take(300).collect(), str::to_owned);
    (reason.to_owned(), message)
}

/// Send the request `build` makes with a bearer token, once more with a new token if the server refused the first.
pub(crate) async fn send(
    client: &BigQueryClient,
    build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, ApiError> {
    let token = access_token(client, None).await?;
    let first = build(&client.http)
        .bearer_auth(token.expose())
        .send()
        .await?;
    if first.status().as_u16() != UNAUTHORIZED {
        return google_response(first).await;
    }
    let token = access_token(client, Some(&token)).await?;
    let again = build(&client.http)
        .bearer_auth(token.expose())
        .send()
        .await?;
    google_response(again).await
}

/// The JSON answer to a request that may be sent again unchanged (a read, or a query carrying its `requestId`), so a transport failure, a 429 or a 5xx is retried with backoff.
pub(crate) async fn json_with_retries(
    client: &BigQueryClient,
    build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
) -> Result<serde_json::Value, ApiError> {
    let policy = RetryPolicy::default();
    let mut attempt = 1;
    loop {
        let answer = match send(client, &build).await {
            Ok(response) => response.json().await.map_err(ApiError::from),
            Err(error) => Err(error),
        };
        match answer {
            Err(error) if error.is_retryable() && attempt < policy.max_attempts => {
                tracing::debug!(%error, attempt, "retrying a BigQuery request");
                tokio::time::sleep(policy.delay(attempt)).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}
