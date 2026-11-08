//! Snowflake OAuth with PKCE: the authorization URL, the redirect it comes back to, the token endpoint, and keeping an access token fresh.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

use crate::auth::_helpers::sign_in::{SignInIo, sign_in};
use crate::auth::_helpers::token_cache::{read_cached, write_cached};
use crate::auth::constants::{DEFAULT_ACCESS_LIFETIME_SECS, OAUTH_REFRESH_MARGIN_SECS};
use crate::auth::errors::TokenError;
use crate::auth::models::{OauthClient, OauthState, OauthTokens, Secret};
use crate::http::errors::ApiError;
use crate::time_text::main::now_ms::now_ms;

/// One sign-in's PKCE verifier, the challenge sent for it, and the `state` that ties the redirect to it.
pub(crate) struct Pkce {
    pub(crate) verifier: String,
    pub(crate) challenge: String,
    pub(crate) state: String,
}

/// The S256 challenge of `verifier` (RFC 7636).
pub(crate) fn challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// A fresh verifier (64 URL-safe characters) and state.
pub(crate) fn new_pkce() -> Result<Pkce, ApiError> {
    let mut verifier = [0_u8; 48];
    let mut state = [0_u8; 16];
    getrandom::fill(&mut verifier)
        .and_then(|()| getrandom::fill(&mut state))
        .map_err(|error| ApiError::Auth(format!("no randomness for the sign-in: {error}")))?;
    let verifier = URL_SAFE_NO_PAD.encode(verifier);
    Ok(Pkce {
        challenge: challenge(&verifier),
        verifier,
        state: URL_SAFE_NO_PAD.encode(state),
    })
}

/// The scope a sign-in asks for: a refresh token, and the configured role (a plain name in upper case, as Snowflake stores it; any other name URL-encoded).
pub(crate) fn scope(role: Option<&str>) -> String {
    let Some(role) = role else {
        return "refresh_token".to_owned();
    };
    let plain = role
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '$');
    match plain {
        true => format!("refresh_token session:role:{}", role.to_ascii_uppercase()),
        false => {
            let encoded: String = role
                .bytes()
                .map(
                    |byte| match byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
                        true => char::from(byte).to_string(),
                        false => format!("%{byte:02X}"),
                    },
                )
                .collect();
            format!("refresh_token session:role-encoded:{encoded}")
        }
    }
}

/// The authorization endpoint's address for one sign-in.
pub(crate) fn authorize_url(
    client: &OauthClient,
    redirect_uri: &str,
    pkce: &Pkce,
) -> Result<String, ApiError> {
    let mut url = reqwest::Url::parse(&format!("{}/oauth/authorize", client.account_url))
        .map_err(|error| ApiError::Auth(format!("bad account URL: {error}")))?;
    url.query_pairs_mut()
        .append_pair("client_id", &client.client_id)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &pkce.state)
        .append_pair("scope", &scope(client.role.as_deref()));
    Ok(url.into())
}

/// The address the sign-in ended on, as a URL: a full URL, or its path and query.
fn redirect_url(redirected: &str) -> Result<reqwest::Url, ApiError> {
    let trimmed = redirected.trim();
    let absolute = match trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        true => trimmed.to_owned(),
        false => format!("http://127.0.0.1/{}", trimmed.trim_start_matches('/')),
    };
    reqwest::Url::parse(&absolute).map_err(|error| {
        ApiError::Auth(format!(
            "that is not the address the sign-in ended on: {error}"
        ))
    })
}

/// `name`'s value in `url`'s query.
fn query_value(url: &reqwest::Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

/// The `state` the address a sign-in ended on carries, if any.
pub(crate) fn redirect_state(redirected: &str) -> Option<String> {
    match redirect_url(redirected) {
        Ok(url) => query_value(&url, "state"),
        Err(_) => None,
    }
}

/// The authorization code in the address the sign-in redirected to, checked against `state`; Snowflake's own error when it sent one, with control characters dropped so it cannot drive the terminal.
pub(crate) fn redirect_code(redirected: &str, state: &str) -> Result<String, ApiError> {
    let url = redirect_url(redirected)?;
    let printable = |text: String| -> String {
        text.chars()
            .filter(|character| !character.is_control())
            .collect()
    };
    if let Some(error) = query_value(&url, "error") {
        let description = query_value(&url, "error_description").unwrap_or_default();
        return Err(ApiError::Auth(format!(
            "Snowflake refused the sign-in: {} {}",
            printable(error),
            printable(description)
        )));
    }
    if query_value(&url, "state").as_deref() != Some(state) {
        return Err(ApiError::Auth(
            "the sign-in came back for another request (state does not match); sign in again"
                .to_owned(),
        ));
    }
    query_value(&url, "code").ok_or_else(|| {
        ApiError::Auth("the address the sign-in ended on has no code= in it".to_owned())
    })
}

/// Send one grant to the token endpoint (the client's ID and any secret in the body); the tokens, checked to belong to the configured user.
pub(crate) async fn token_request(
    client: &OauthClient,
    grant: &[(&str, &str)],
) -> Result<OauthTokens, TokenError> {
    let mut form: Vec<(&str, &str)> = grant.to_vec();
    form.push(("client_id", &client.client_id));
    if let Some(secret) = &client.client_secret {
        form.push(("client_secret", secret.expose()));
    }
    let response = client
        .http
        .post(format!("{}/oauth/token-request", client.account_url))
        .form(&form)
        .send()
        .await
        .map_err(|error| TokenError::Failed(error.into()))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| TokenError::Failed(error.into()))?;
    let body: serde_json::Value = match serde_json::from_str(&text) {
        Ok(body) => body,
        Err(_) => serde_json::Value::Null,
    };
    if !status.is_success() {
        let reason = format!(
            "{} {}",
            body["error"].as_str().unwrap_or(status.as_str()),
            body["message"].as_str().unwrap_or(&text)
        );
        return Err(match status.as_u16() {
            400 | 401 => TokenError::Refused(reason),
            _ => TokenError::Failed(ApiError::Auth(format!(
                "the Snowflake token endpoint failed: {reason}"
            ))),
        });
    }
    tokens_from(client, &body).map_err(|reason| TokenError::Failed(ApiError::Auth(reason)))
}

/// The tokens in a token endpoint answer, or why they cannot be used.
fn tokens_from(client: &OauthClient, body: &serde_json::Value) -> Result<OauthTokens, String> {
    let access_token = body["access_token"]
        .as_str()
        .ok_or("the Snowflake token endpoint sent no access_token")?;
    if let Some(username) = body["username"].as_str()
        && !username.eq_ignore_ascii_case(&client.user)
    {
        return Err(format!(
            "signed in to Snowflake as {username}, but this connection's user is {}; sign in as {} instead",
            client.user, client.user
        ));
    }
    let now = now_secs();
    Ok(OauthTokens {
        access_token: Secret(access_token.to_owned()),
        renew_after: renew_after(now, body["expires_in"].as_u64()),
        refresh_token: body["refresh_token"]
            .as_str()
            .map(|token| Secret(token.to_owned())),
        refresh_expires_at: body["refresh_token_expires_in"]
            .as_u64()
            .map(|seconds| now + seconds),
    })
}

/// The access token to send now. In order: the one held, while its renewal time has not come; newer tokens another connection or process cached; one renewed with the refresh token (keeping the refresh token when Snowflake sends no new one); one from a new browser sign-in. Every new token is cached. Only a refused refresh falls through to signing in, and a failed sign-in fails every later request of the run at once, so nobody is asked twice.
pub(crate) async fn fresh_access_token(
    client: &OauthClient,
    state: &tokio::sync::Mutex<OauthState>,
    io: &SignInIo,
) -> Result<String, ApiError> {
    let mut held = state.lock().await;
    if let Some(failure) = &held.failure {
        return Err(ApiError::Auth(failure.clone()));
    }
    let now = now_secs();
    if let Some(access) = usable_access(held.tokens.as_ref(), now) {
        return Ok(access);
    }
    let newer = read_cached(client).filter(|cached| {
        held.tokens
            .as_ref()
            .is_none_or(|current| cached.renew_after > current.renew_after)
    });
    if newer.is_some() {
        held.tokens = newer;
    }
    if let Some(access) = usable_access(held.tokens.as_ref(), now) {
        return Ok(access);
    }
    let refreshed = refresh(client, held.tokens.as_ref(), now).await?;
    let renewed = match refreshed {
        Some(renewed) => renewed,
        None => match sign_in(client, io).await {
            Ok(signed_in) => signed_in,
            Err(error) => {
                held.failure = Some(error.to_string());
                return Err(error);
            }
        },
    };
    if let Err(error) = write_cached(client, &renewed) {
        tracing::warn!(%error, "cannot cache the Snowflake sign-in; the next run signs in again");
    }
    let access = renewed.access_token.expose().to_owned();
    held.tokens = Some(renewed);
    Ok(access)
}

/// The access token of `tokens` while its renewal time has not come.
fn usable_access(tokens: Option<&OauthTokens>, now: u64) -> Option<String> {
    tokens
        .filter(|current| current.renew_after > now)
        .map(|current| current.access_token.expose().to_owned())
}

/// Tokens renewed with the refresh token of `current`, keeping it when Snowflake sends no new one; `None` when there is no unexpired refresh token or Snowflake refused it.
async fn refresh(
    client: &OauthClient,
    current: Option<&OauthTokens>,
    now: u64,
) -> Result<Option<OauthTokens>, ApiError> {
    let Some((refresh, expires)) = current.and_then(|current| {
        current
            .refresh_token
            .clone()
            .filter(|_| {
                current
                    .refresh_expires_at
                    .is_none_or(|expires| expires > now)
            })
            .map(|token| (token, current.refresh_expires_at))
    }) else {
        return Ok(None);
    };
    let grant = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh.expose()),
    ];
    match token_request(client, &grant).await {
        Ok(mut renewed) => {
            if renewed.refresh_token.is_none() {
                renewed.refresh_token = Some(refresh);
                renewed.refresh_expires_at = expires;
            }
            Ok(Some(renewed))
        }
        Err(TokenError::Refused(reason)) => {
            tracing::info!(%reason, "Snowflake refused the cached refresh token; signing in again");
            Ok(None)
        }
        Err(TokenError::Failed(error)) => Err(error),
    }
}

/// When to renew an access token issued at `now` for `lifetime` seconds (Snowflake's default of 600 when not given): a minute before it expires, or halfway through for a token of under two minutes.
pub(crate) fn renew_after(now: u64, lifetime: Option<u64>) -> u64 {
    let lifetime = lifetime.unwrap_or(DEFAULT_ACCESS_LIFETIME_SECS);
    now + lifetime - (lifetime / 2).min(OAUTH_REFRESH_MARGIN_SECS)
}

/// Now, in Unix seconds.
pub(crate) fn now_secs() -> u64 {
    u64::try_from(now_ms() / 1000).unwrap_or(0)
}
