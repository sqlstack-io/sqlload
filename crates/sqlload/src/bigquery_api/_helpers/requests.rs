//! Authorized requests, with the token fetched and kept until shortly before it expires, and Google's error bodies read.

use crate::auth::models::Secret;
use crate::bigquery_api::constants::{RATE_LIMITED, SCOPE, UNAUTHORIZED};
use crate::bigquery_api::models::BigQueryClient;
use crate::google_identity::main::google_access_token::google_access_token;
use crate::http::constants::TOO_MANY_REQUESTS;
use crate::http::errors::ApiError;
use crate::http::models::RetryPolicy;

/// A BigQuery token for the client: see [`google_access_token`].
pub(crate) async fn access_token(
    client: &BigQueryClient,
    refused: Option<&Secret>,
) -> Result<Secret, ApiError> {
    google_access_token(
        &client.http,
        &client.credentials,
        &client.token,
        (SCOPE, refused),
    )
    .await
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
