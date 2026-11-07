//! Authorized requests, with the token fetched and kept until shortly before it expires.

use crate::auth::models::Secret;
use crate::databricks_api::constants::{TOKEN_PATH, TOKEN_REFRESH_MARGIN_MS, UNAUTHORIZED};
use crate::databricks_api::models::DatabricksClient;
use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::http::models::RetryPolicy;
use crate::time_text::main::now_ms::now_ms;

/// A token for the client's credentials: the kept one, or a new one when it is missing, near its expiry, or the one the server `refused`; a request refused with a token another request has already replaced gets the replacement.
pub(crate) async fn access_token(
    client: &DatabricksClient,
    refused: Option<&Secret>,
) -> Result<Secret, ApiError> {
    let mut kept = client.token.lock().await;
    if let Some((token, expires_ms)) = kept.as_ref()
        && refused.is_none_or(|refused| refused.expose() != token.expose())
        && now_ms() + TOKEN_REFRESH_MARGIN_MS < *expires_ms
    {
        return Ok(token.clone());
    }
    let sent = client
        .http
        .post(format!("{}{TOKEN_PATH}", client.host))
        .basic_auth(&client.client_id, Some(client.client_secret.expose()))
        .form(&[("grant_type", "client_credentials"), ("scope", "all-apis")])
        .send()
        .await?;
    let response = check_response(sent).await.map_err(|error| {
        ApiError::Auth(format!(
            "Databricks refused the OAuth client credentials: {error}"
        ))
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

/// Send the request `build` makes with a bearer token, once more with a new token if the server refused the first; a non-success answer becomes an [`ApiError`].
pub(crate) async fn send(
    client: &DatabricksClient,
    build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, ApiError> {
    let token = access_token(client, None).await?;
    let first = build(&client.http)
        .bearer_auth(token.expose())
        .send()
        .await?;
    if first.status().as_u16() != UNAUTHORIZED {
        return check_response(first).await;
    }
    let token = access_token(client, Some(&token)).await?;
    let again = build(&client.http)
        .bearer_auth(token.expose())
        .send()
        .await?;
    check_response(again).await
}

/// `GET url`, which changes nothing, so a transport failure, a 429 or a 5xx is retried with backoff.
pub(crate) async fn json_with_retries(
    client: &DatabricksClient,
    url: &str,
) -> Result<serde_json::Value, ApiError> {
    let policy = RetryPolicy::default();
    let mut attempt = 1;
    loop {
        let answer = match send(client, |http| http.get(url)).await {
            Ok(response) => response.json().await.map_err(ApiError::from),
            Err(error) => Err(error),
        };
        match answer {
            Err(error) if error.is_retryable() && attempt < policy.max_attempts => {
                tracing::debug!(%error, attempt, url, "retrying a Databricks request");
                tokio::time::sleep(policy.delay(attempt)).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}
