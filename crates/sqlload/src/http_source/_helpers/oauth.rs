//! OAuth2 access tokens: fetched from the token URL, shared by the run's jobs, renewed before they expire and after a 401.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use base64::Engine;
use reqwest::Url;
use serde_json::Value;

use crate::http::constants::{FIRST_SERVER_ERROR, TOO_MANY_REQUESTS};
use crate::http::models::RetryPolicy;
use crate::http_source::_helpers::fetch::{jittered, transport_text};
use crate::http_source::_helpers::urls::{encoded, redacted};
use crate::http_source::constants::{
    ERROR_EXCERPT_CHARS, FIRST_RETRY_DELAY, MAX_RETRY_DELAY, MAX_TOKEN_LIFETIME,
    MAX_TOKEN_RENEWAL_MARGIN,
};
use crate::http_source::models::{ClientAuth, Grant, Issued, OAuth2Spec, TokenSource, TokenState};
use crate::python_sources::errors::ReadError;

/// `spec`'s token URL, which must be `https`, or `http` on a loopback host, because the client's secret goes there.
pub(crate) fn token_url(spec: &OAuth2Spec) -> Result<Url, String> {
    let url = Url::parse(&spec.token_url)
        .map_err(|error| format!("token_url {} is not a URL: {error}", spec.token_url))?;
    let host = url.host_str().unwrap_or_default();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    match (url.scheme(), loopback) {
        ("https", _) | ("http", true) => Ok(url),
        _ => Err(format!(
            "token_url {} must be https (http only on localhost), because the client's credentials are sent there",
            spec.token_url
        )),
    }
}

/// The token source every job of `connection` with the same auth `key` shares in this process, so one load fetches one token.
pub(crate) fn shared_token_source(
    connection: &str,
    key: &str,
    spec: &OAuth2Spec,
    grant: Grant,
) -> Result<Arc<TokenSource>, String> {
    static SOURCES: OnceLock<Mutex<HashMap<String, Arc<TokenSource>>>> = OnceLock::new();
    let token_url = token_url(spec)?;
    let mut sources = SOURCES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| "the OAuth2 tokens' lock is poisoned".to_owned())?;
    let source = sources
        .entry(format!("{connection}\u{0}{key}"))
        .or_insert_with(|| Arc::new(token_source(token_url, spec, grant)));
    Ok(source.clone())
}

fn token_source(token_url: Url, spec: &OAuth2Spec, grant: Grant) -> TokenSource {
    let (grant_type, client, refresh_token) = match grant {
        Grant::ClientCredentials {
            client_id,
            client_secret,
        } => (
            "client_credentials",
            (Some(client_id), Some(client_secret)),
            None,
        ),
        Grant::RefreshToken {
            refresh_token,
            client,
        } => ("refresh_token", client, Some(refresh_token)),
    };
    let mut form = vec![("grant_type".to_owned(), grant_type.to_owned())];
    form.extend(spec.scope.clone().map(|scope| ("scope".to_owned(), scope)));
    let mut secrets: Vec<String> = refresh_token
        .iter()
        .flat_map(|token| secret_forms(token))
        .collect();
    let basic = match (client, spec.client_auth) {
        ((Some(id), Some(secret)), ClientAuth::Basic) => {
            let pair = (encoded(&id), encoded(&secret));
            secrets.extend(secret_forms(&secret));
            secrets.push(
                base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", pair.0, pair.1)),
            );
            Some(pair)
        }
        ((id, secret), _) => {
            form.extend(id.map(|id| ("client_id".to_owned(), id)));
            secrets.extend(secret.iter().flat_map(|secret| secret_forms(secret)));
            form.extend(secret.map(|secret| ("client_secret".to_owned(), secret)));
            None
        }
    };
    secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    TokenSource {
        token_url,
        form,
        basic,
        state: tokio::sync::Mutex::new(TokenState {
            refresh_token,
            ..TokenState::default()
        }),
        secrets: Mutex::new(secrets),
    }
}

impl TokenSource {
    /// Every secret of the token flow so far: the client secret, refresh tokens and each access token issued.
    pub(crate) fn secrets(&self) -> Vec<String> {
        self.secrets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether `access` is still the cached token and was issued longer than `age` ago.
    pub(crate) async fn issued_before(&self, access: &str, age: Duration) -> bool {
        let state = self.state.lock().await;
        state.access.as_deref() == Some(access)
            && state.issued_at.is_some_and(|issued| issued.elapsed() > age)
    }

    /// The access token to send: the cached one while it is fresh and is not `refused`, else a new one from the token URL.
    pub(crate) async fn access_token(
        &self,
        http: &reqwest::Client,
        retries: u32,
        refused: Option<&str>,
    ) -> Result<String, ReadError> {
        let mut state = self.state.lock().await;
        let fresh = state
            .renew_at
            .is_none_or(|renew_at| Instant::now() < renew_at);
        match &state.access {
            Some(access) if fresh && Some(access.as_str()) != refused => return Ok(access.clone()),
            Some(_) | None => {}
        }
        let issued = self.request(http, retries, &state).await?;
        let renew_at = issued
            .renew_after
            .and_then(|delay| Instant::now().checked_add(delay));
        let mut secrets = self.secrets.lock().unwrap_or_else(PoisonError::into_inner);
        for secret in std::iter::once(&issued.access).chain(&issued.refresh_token) {
            secrets.extend(secret_forms(secret));
        }
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        drop(secrets);
        state.access = Some(issued.access.clone());
        state.issued_at = Some(Instant::now());
        state.renew_at = renew_at;
        if state.refresh_token.is_some() {
            state.refresh_token = issued.refresh_token.or(state.refresh_token.take());
        }
        Ok(issued.access)
    }

    /// One token request, retried like a page on transport errors, 429 and 5xx.
    async fn request(
        &self,
        http: &reqwest::Client,
        retries: u32,
        state: &TokenState,
    ) -> Result<Issued, ReadError> {
        let policy = RetryPolicy {
            first_delay: FIRST_RETRY_DELAY,
            max_delay: MAX_RETRY_DELAY,
            max_attempts: retries + 1,
        };
        let shown = format!("POST {} (OAuth2 token)", self.token_url);
        let mut form = self.form.clone();
        form.extend(
            state
                .refresh_token
                .clone()
                .map(|token| ("refresh_token".to_owned(), token)),
        );
        let mut attempt = 1;
        loop {
            let mut request = http.post(self.token_url.clone()).form(&form);
            if let Some((id, secret)) = &self.basic {
                request = request.basic_auth(id, Some(secret));
            }
            let failure = match request.send().await {
                Ok(response) => match self.issued(&shown, response).await? {
                    Ok(issued) => return Ok(issued),
                    Err(failure) => failure,
                },
                Err(error) => redacted(
                    &format!("{shown}: {}", transport_text(error)),
                    &self.secrets(),
                ),
            };
            if attempt >= policy.max_attempts {
                return Err(ReadError::transient(format!(
                    "{failure} (after {} attempts)",
                    policy.max_attempts
                )));
            }
            tokio::time::sleep(jittered(policy.delay(attempt))).await;
            attempt += 1;
        }
    }

    /// The token a response issued, or `Ok(Err(reason))` for a failure worth retrying.
    async fn issued(
        &self,
        shown: &str,
        response: reqwest::Response,
    ) -> Result<Result<Issued, String>, String> {
        let status = response.status().as_u16();
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return Ok(Err(format!(
                    "{shown}: reading the body failed: {}",
                    transport_text(error)
                )));
            }
        };
        let secrets = self.secrets();
        let failed = |status: u16| {
            let reason = match serde_json::from_slice::<Value>(&body) {
                Ok(document) => oauth_error(&document)
                    .unwrap_or_else(|| String::from_utf8_lossy(&body).into_owned()),
                Err(_) => String::from_utf8_lossy(&body).into_owned(),
            };
            let reason = redacted(&reason, &secrets)
                .chars()
                .take(ERROR_EXCERPT_CHARS)
                .collect::<String>();
            format!("{shown}: HTTP {status}: {reason}")
        };
        match status {
            200..=299 => {}
            TOO_MANY_REQUESTS | FIRST_SERVER_ERROR.. => return Ok(Err(failed(status))),
            _ => {
                return Err(format!(
                    "{}; check the connection's OAuth2 credentials and the token_url",
                    failed(status)
                ));
            }
        }
        let document: Value = serde_json::from_slice(&body)
            .map_err(|error| format!("{shown}: the token response is not JSON: {error}"))?;
        let text = |key: &str| {
            document
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .filter(|value| !value.is_empty())
        };
        let access = text("access_token")
            .ok_or_else(|| format!("{shown}: the response has no access_token"))?;
        match text("token_type") {
            Some(kind) if !kind.eq_ignore_ascii_case("bearer") => {
                return Err(format!(
                    "{shown}: token_type {kind} is not supported; only bearer tokens are"
                ));
            }
            Some(_) | None => {}
        }
        let renew_after = renewal_delay(document.get("expires_in"))
            .map_err(|error| format!("{shown}: {error}"))?;
        Ok(Ok(Issued {
            access,
            renew_after,
            refresh_token: text("refresh_token").filter(|token| !token.is_empty()),
        }))
    }
}

/// The OAuth2 `error` and `error_description` of a token endpoint's failure, when it gives them.
fn oauth_error(document: &Value) -> Option<String> {
    let text = |key: &str| document.get(key).and_then(Value::as_str);
    let error = text("error")?;
    Some(match text("error_description") {
        Some(description) => format!("{error}: {description}"),
        None => error.to_owned(),
    })
}

/// How long after issue a token with this `expires_in` is renewed: its lifetime less min(60 s, a tenth of it), the lifetime capped at a year; `None` when the response gives no positive lifetime.
pub(crate) fn renewal_delay(expires_in: Option<&Value>) -> Result<Option<Duration>, String> {
    let seconds = match expires_in {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Number(seconds)) => seconds.as_f64(),
        Some(Value::String(seconds)) => Some(
            seconds
                .trim()
                .parse::<f64>()
                .map_err(|_| format!("expires_in {seconds:?} is not a number of seconds"))?,
        ),
        Some(other) => return Err(format!("expires_in {other} is not a number of seconds")),
    };
    Ok(seconds
        .filter(|seconds| seconds.is_normal() && seconds.is_sign_positive())
        .map(|seconds| {
            let lifetime = Duration::from_secs_f64(seconds.min(MAX_TOKEN_LIFETIME.as_secs_f64()));
            lifetime - (lifetime / 10).min(MAX_TOKEN_RENEWAL_MARGIN)
        }))
}

/// `secret` as sent and as it appears in a URL or a form body, for redaction.
fn secret_forms(secret: &str) -> [String; 3] {
    let percent = encoded(secret);
    let form = percent
        .replace("%20", "+")
        .replace("%2A", "*")
        .replace('~', "%7E");
    [secret.to_owned(), percent, form]
}
