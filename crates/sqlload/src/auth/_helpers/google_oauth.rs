//! A Google user's access tokens for an installed app's OAuth client: one consent in the browser, the refresh token it gives kept, and access tokens renewed with it.

use crate::auth::_helpers::oauth::{Pkce, now_secs, renew_after};
use crate::auth::_helpers::sign_in::{Consent, SignInIo, browser_code, say};
use crate::auth::_helpers::token_cache::{read_entry, write_entry};
use crate::auth::constants::{DEFAULT_GOOGLE_TOKEN_SECS, GOOGLE_PROVIDER, INVALID_GRANT};
use crate::auth::errors::TokenError;
use crate::auth::models::{GoogleUser, OauthTokens, Secret};
use crate::http::constants::TOO_MANY_REQUESTS;
use crate::http::errors::ApiError;
use crate::time_text::main::now_ms::now_ms;

/// The access token to send now, other than `refused`: the one `held`, while it has a minute left; else, with the connection's `refresh_token`, a renewed one; else the cached one, or one renewed with the cached refresh token, or one from a new consent in the browser through `io`, which is cached. A refresh token Google refuses is an error naming why, as is a consent without a terminal.
pub(crate) async fn google_access_token(
    http: &reqwest::Client,
    user: &GoogleUser,
    held: &tokio::sync::Mutex<Option<(Secret, i64)>>,
    (refused, io): (Option<&Secret>, &SignInIo),
) -> Result<Secret, ApiError> {
    let mut held = held.lock().await;
    let now = now_ms();
    let current = held
        .as_ref()
        .filter(|(token, expires)| *expires > now && Some(token) != refused);
    if let Some((token, _)) = current {
        return Ok(token.clone());
    }
    let tokens = match &user.refresh_token {
        Some(configured) => renew(http, user, configured)
            .await
            .map_err(|error| configured_refused(user, error))?,
        None => cached_or_consent(http, user, (refused, io)).await?,
    };
    let expires = i64::try_from(tokens.renew_after)
        .unwrap_or(i64::MAX / 1000)
        .saturating_mul(1000);
    *held = Some((tokens.access_token.clone(), expires));
    Ok(tokens.access_token)
}

/// The cached access token when it is not `refused` and has a minute left; else one renewed with the cached refresh token; else a new consent's. New tokens are cached.
async fn cached_or_consent(
    http: &reqwest::Client,
    user: &GoogleUser,
    (refused, io): (Option<&Secret>, &SignInIo),
) -> Result<OauthTokens, ApiError> {
    let key = cache_key(user);
    let cached = read_entry(&user.cache_file, &key);
    let now = now_secs();
    if let Some(cached) = cached
        .as_ref()
        .filter(|cached| cached.renew_after > now && Some(&cached.access_token) != refused)
    {
        return Ok(cached.clone());
    }
    let refresh = cached.and_then(|cached| {
        cached.refresh_token.filter(|_| {
            cached
                .refresh_expires_at
                .is_none_or(|expires| expires > now)
        })
    });
    let renewed = match refresh {
        Some(refresh) => match renew(http, user, &refresh).await {
            Ok(renewed) => Ok(renewed),
            Err(TokenError::Refused(reason)) => Err(Some(reason)),
            Err(TokenError::Failed(error)) => return Err(error),
        },
        None => Err(None),
    };
    let tokens = match renewed {
        Ok(renewed) => renewed,
        Err(refusal) => consent(http, user, io).await.map_err(|error| match refusal {
            Some(reason) => ApiError::Auth(format!(
                "Google refused the cached refresh token of connection {:?} ({reason}), so it needs a new sign-in: {error}",
                user.connection
            )),
            None => error,
        })?,
    };
    if let Err(error) = write_entry(&user.cache_file, &key, &tokens) {
        tracing::warn!(%error, "cannot cache the Google sign-in; the next run signs in again");
    }
    Ok(tokens)
}

/// The key a connection's Google sign-in is cached under: its OAuth client and its name.
pub(crate) fn cache_key(user: &GoogleUser) -> String {
    format!("{} {}", user.client_id, user.connection)
}

/// The connection's own refresh token's failure, saying how to replace a refused one.
fn configured_refused(user: &GoogleUser, error: TokenError) -> ApiError {
    match error {
        TokenError::Refused(reason) => ApiError::Auth(format!(
            "Google refused the refresh_token of connection {:?} ({reason}): it was revoked, has expired (an OAuth client whose consent screen is in testing gets tokens that last 7 days), or belongs to another client; get a new one by signing in from a terminal with refresh_token unset",
            user.connection
        )),
        TokenError::Failed(error) => error,
    }
}

/// Tokens renewed with `refresh`, which they keep, as Google sends no new one.
async fn renew(
    http: &reqwest::Client,
    user: &GoogleUser,
    refresh: &Secret,
) -> Result<OauthTokens, TokenError> {
    let grant = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh.expose()),
    ];
    let mut renewed = token_grant(http, user, &grant).await?;
    if renewed.refresh_token.is_none() {
        renewed.refresh_token = Some(refresh.clone());
    }
    Ok(renewed)
}

/// Have the person consent in the browser and exchange the code for tokens, which must hold a refresh token.
async fn consent(
    http: &reqwest::Client,
    user: &GoogleUser,
    io: &SignInIo,
) -> Result<OauthTokens, ApiError> {
    let who = format!("the Google account for connection {:?}", user.connection);
    let consent = Consent {
        provider: GOOGLE_PROVIDER,
        who: &who,
        redirect_uri: None,
        unattended: "run `sqlload plan` from a terminal once to sign in, or set the connection's refresh_token for unattended runs",
    };
    let signed = browser_code(&consent, io, |redirect_uri, pkce| {
        authorize_url(user, redirect_uri, pkce)
    })
    .await?;
    let grant = [
        ("grant_type", "authorization_code"),
        ("code", signed.code.as_str()),
        ("redirect_uri", signed.redirect_uri.as_str()),
        ("code_verifier", signed.verifier.as_str()),
    ];
    let tokens = token_grant(http, user, &grant)
        .await
        .map_err(|error| match error {
            TokenError::Refused(reason) => {
                ApiError::Auth(format!("Google refused the sign-in code: {reason}"))
            }
            TokenError::Failed(error) => error,
        })?;
    if tokens.refresh_token.is_none() {
        return Err(ApiError::Auth(
            "Google sent no refresh token for the sign-in; remove SQLLoad's access at https://myaccount.google.com/permissions and sign in again".to_owned(),
        ));
    }
    say(&format!(
        "Signed in to Google for connection {:?}; the refresh token is cached in {}.",
        user.connection,
        user.cache_file.display()
    ));
    Ok(tokens)
}

/// The consent page's address: the user's scope, offline so Google sends a refresh token, with the consent screen shown each time so it always does.
fn authorize_url(user: &GoogleUser, redirect_uri: &str, pkce: &Pkce) -> Result<String, ApiError> {
    let mut url = reqwest::Url::parse(&user.authorize_url)
        .map_err(|error| ApiError::Auth(format!("bad Google authorization URL: {error}")))?;
    url.query_pairs_mut()
        .append_pair("client_id", &user.client_id)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", &user.scope)
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &pkce.state)
        .append_pair("access_type", "offline")
        .append_pair("prompt", "consent");
    Ok(url.into())
}

/// Send one grant, with the client's ID and secret, to Google's token endpoint. `invalid_grant` is a refusal; throttling and server errors keep their status, so they are retried; any other refusal names the client.
async fn token_grant(
    http: &reqwest::Client,
    user: &GoogleUser,
    grant: &[(&str, &str)],
) -> Result<OauthTokens, TokenError> {
    let mut form: Vec<(&str, &str)> = grant.to_vec();
    form.push(("client_id", &user.client_id));
    form.push(("client_secret", user.client_secret.expose()));
    let response = http
        .post(&user.token_url)
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
    let error = body["error"].as_str().unwrap_or(status.as_str()).to_owned();
    let description = body["error_description"]
        .as_str()
        .unwrap_or(&text)
        .to_owned();
    match status.as_u16() {
        _ if status.is_success() => tokens_from(&body).map_err(TokenError::Failed),
        _ if error == INVALID_GRANT => Err(TokenError::Refused(format!("{error}: {description}"))),
        code if code == TOO_MANY_REQUESTS || status.is_server_error() => {
            Err(TokenError::Failed(ApiError::Status {
                status: code,
                code: error,
                message: format!("Google's token endpoint: {description}"),
            }))
        }
        _ => Err(TokenError::Failed(ApiError::Auth(format!(
            "Google's token endpoint refused OAuth client {} ({error}: {description}); check the connection's client_id and client_secret",
            user.client_id
        )))),
    }
}

/// The tokens in a token endpoint answer.
fn tokens_from(body: &serde_json::Value) -> Result<OauthTokens, ApiError> {
    let access_token = body["access_token"].as_str().ok_or_else(|| {
        ApiError::Protocol("Google's token endpoint sent no access_token".to_owned())
    })?;
    let now = now_secs();
    let lifetime = body["expires_in"]
        .as_u64()
        .unwrap_or(DEFAULT_GOOGLE_TOKEN_SECS);
    Ok(OauthTokens {
        access_token: Secret(access_token.to_owned()),
        renew_after: renew_after(now, Some(lifetime)),
        refresh_token: body["refresh_token"]
            .as_str()
            .map(|token| Secret(token.to_owned())),
        refresh_expires_at: body["refresh_token_expires_in"]
            .as_u64()
            .map(|seconds| now.saturating_add(seconds)),
    })
}
