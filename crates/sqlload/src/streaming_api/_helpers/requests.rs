use std::time::Instant;

use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::streaming_api::constants::{HTTP_SCHEME_PREFIX, SCOPED_TOKEN_LIFETIME, UNAUTHORIZED};
use crate::streaming_api::models::{IngestAccess, StreamingClient};

/// `databases/{db}/schemas/{schema}/pipes/{pipe}`.
pub(crate) fn pipe_path(client: &StreamingClient, pipe: &str) -> String {
    format!(
        "databases/{}/schemas/{}/pipes/{pipe}",
        client.database, client.schema
    )
}

/// Send to the ingest host, refreshing access once on 401.
pub(crate) async fn send(
    client: &StreamingClient,
    method: reqwest::Method,
    path: &str,
    build: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, ApiError> {
    let mut refreshed = false;
    loop {
        let access = access(client).await?;
        let request = client
            .http
            .request(method.clone(), format!("{}{path}", access.base))
            .query(&[("requestId", uuid::Uuid::new_v4().to_string())]);
        let request = authorize(client, request, &access)?;
        match check_response(build(request).send().await?).await {
            Err(error) if error.status() == Some(UNAUTHORIZED) && !refreshed => {
                refreshed = true;
                *client.access.lock().await = None;
            }
            outcome => return outcome,
        }
    }
}

fn authorize(
    client: &StreamingClient,
    request: reqwest::RequestBuilder,
    access: &IngestAccess,
) -> Result<reqwest::RequestBuilder, ApiError> {
    if let Some(token) = &access.scoped_token {
        return Ok(request.bearer_auth(token));
    }
    let bearer = client.auth.bearer()?;
    Ok(request
        .bearer_auth(bearer.token)
        .header("X-Snowflake-Authorization-Token-Type", bearer.token_type))
}

/// The cached ingest host and token, discovered again when the scoped token is old.
pub(crate) async fn access(client: &StreamingClient) -> Result<IngestAccess, ApiError> {
    let mut guard = client.access.lock().await;
    if let Some(access) = guard.as_ref()
        && (access.scoped_token.is_none() || access.fetched.elapsed() < SCOPED_TOKEN_LIFETIME)
    {
        return Ok(access.clone());
    }
    let host = discover_host(client).await?;
    let scheme = match client.account_url.starts_with(HTTP_SCHEME_PREFIX) {
        true => "http",
        false => "https",
    };
    let scoped_token = match client.auth.uses_key_pair() {
        true => Some(scoped_token(client, &host).await?),
        false => None,
    };
    let access = IngestAccess {
        base: format!("{scheme}://{host}"),
        scoped_token,
        fetched: Instant::now(),
    };
    *guard = Some(access.clone());
    Ok(access)
}

async fn discover_host(client: &StreamingClient) -> Result<String, ApiError> {
    let bearer = client.auth.bearer()?;
    let response = client
        .http
        .get(format!("{}/v2/streaming/hostname", client.account_url))
        .bearer_auth(bearer.token)
        .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
        .send()
        .await?;
    let text = check_response(response).await?.text().await?;
    Ok(text.trim().trim_matches('"').replace('_', "-"))
}

async fn scoped_token(client: &StreamingClient, host: &str) -> Result<String, ApiError> {
    let bearer = client.auth.bearer()?;
    let response = client
        .http
        .post(format!("{}/oauth/token", client.account_url))
        .bearer_auth(bearer.token)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("scope", host),
        ])
        .send()
        .await?;
    let text = check_response(response).await?.text().await?;
    Ok(token_from_body(&text))
}

fn token_from_body(text: &str) -> String {
    let parsed: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return text.trim().to_owned(),
    };
    let token = parsed["access_token"]
        .as_str()
        .or_else(|| parsed["token"].as_str());
    token.map_or_else(|| text.trim().to_owned(), str::to_owned)
}
