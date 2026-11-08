//! One GET with the shared rate limit, retries with backoff, `Retry-After`, and `on_status`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Url;
use reqwest::header::{LINK, RETRY_AFTER};

use crate::http::constants::{FIRST_SERVER_ERROR, TOO_MANY_REQUESTS, UNAUTHORIZED};
use crate::http::models::RetryPolicy;
use crate::http_source::_helpers::urls::{link_header_next, redacted, same_origin};
use crate::http_source::constants::{
    ERROR_EXCERPT_CHARS, FIRST_RETRY_DELAY, MAX_REDIRECTS, MAX_RETRY_AFTER, MAX_RETRY_DELAY,
};
use crate::http_source::models::{Attempt, Calls, Credential, Fetched, StatusAction};

/// The body at `url`, or `None` when `on_status` skips its status; transport errors, 429 and 5xx are retried up to `calls.retries` times.
pub(crate) async fn fetch(calls: &Calls, url: &Url) -> Result<Option<Fetched>, String> {
    let policy = RetryPolicy {
        first_delay: FIRST_RETRY_DELAY,
        max_delay: MAX_RETRY_DELAY,
        max_attempts: calls.retries + 1,
    };
    let mut attempt = 1;
    let mut refused: Option<String> = None;
    let mut renewed = false;
    loop {
        if let Some(limiter) = &calls.limiter {
            limiter.wait().await;
        }
        let access = match &calls.endpoint.credential {
            Credential::OAuth2(tokens) => Some(
                tokens
                    .access_token(&calls.http, calls.retries, refused.as_deref())
                    .await?,
            ),
            _ => None,
        };
        match once(calls, url, access.as_deref(), !renewed).await? {
            Attempt::Done(fetched) => return Ok(fetched),
            Attempt::Refused => {
                refused = access;
                renewed = true;
            }
            Attempt::Retry { reason, .. } if attempt >= policy.max_attempts => {
                return Err(format!("{reason} (after {} attempts)", policy.max_attempts));
            }
            Attempt::Retry { after, .. } => {
                let delay = after.unwrap_or_else(|| jittered(policy.delay(attempt)));
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

/// One try at `url`, with the OAuth2 `access` token when the connection uses one; a 401 comes back as `Refused` while `may_renew`.
async fn once(
    calls: &Calls,
    url: &Url,
    access: Option<&str>,
    may_renew: bool,
) -> Result<Attempt, String> {
    let endpoint = &calls.endpoint;
    let secrets = endpoint.current_secrets();
    let shown = redacted(&format!("GET {url}"), &secrets);
    let own = same_origin(&endpoint.base_url, url);
    let mut request = calls.http.get(url.clone());
    for (name, value) in endpoint.headers.iter().filter(|_| own) {
        request = request.header(name, value);
    }
    request = match (&endpoint.credential, own) {
        (Credential::Header { name, value }, true) => request.header(name, value),
        (Credential::Query { name, value }, true) => request.query(&[(name, value)]),
        (Credential::OAuth2(_), true) => request.bearer_auth(access.unwrap_or_default()),
        _ => request,
    };
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) if error.is_builder() => {
            return Err(format!("{shown}: {}", transport_text(error)));
        }
        Err(error) => {
            return Ok(Attempt::Retry {
                reason: format!("{shown}: {}", transport_text(error)),
                after: None,
            });
        }
    };
    let status = response.status().as_u16();
    let landed = response.url().clone();
    let after = retry_after(&response);
    let next_link = match response.headers().get(LINK).map(|link| link.to_str()) {
        Some(Ok(link)) => link_header_next(link),
        Some(Err(_)) | None => None,
    };
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(error) => {
            return Ok(Attempt::Retry {
                reason: format!(
                    "{shown}: reading the body failed: {}",
                    transport_text(error)
                ),
                after: None,
            });
        }
    };
    let skipped = calls.on_status.get(&status) == Some(&StatusAction::Skip);
    let excerpt = || {
        redacted(&String::from_utf8_lossy(&body), &secrets)
            .chars()
            .take(ERROR_EXCERPT_CHARS)
            .collect::<String>()
    };
    Ok(match status {
        200..=299 => Attempt::Done(Some(Fetched {
            bytes: body.to_vec(),
            next_link,
            url: landed,
        })),
        UNAUTHORIZED if may_renew && access.is_some() => Attempt::Refused,
        _ if skipped => Attempt::Done(None),
        300..=399 => {
            return Err(format!(
                "{shown}: HTTP {status}: the redirect was not followed: it leads to another origin, has no Location, or goes past {MAX_REDIRECTS} hops; credentials never leave the connection's origin, so use the final URL as base_url"
            ));
        }
        TOO_MANY_REQUESTS | FIRST_SERVER_ERROR.. => Attempt::Retry {
            reason: format!("{shown}: HTTP {status}: {}", excerpt()),
            after,
        },
        _ => {
            return Err(format!(
                "{shown}: HTTP {status}: {}; list the status in on_status to skip it",
                excerpt()
            ));
        }
    })
}

/// The wait a `Retry-After` header in seconds asks for, capped.
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let header = response.headers().get(RETRY_AFTER)?;
    let seconds = match header.to_str().map(|text| text.trim().parse::<u64>()) {
        Ok(Ok(seconds)) => seconds,
        Ok(Err(_)) | Err(_) => return None,
    };
    Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

/// `delay` scaled by a factor between 0.5 and 1, so retries of parallel requests spread out.
pub(crate) fn jittered(delay: Duration) -> Duration {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_micros());
    delay.mul_f64(0.5 + f64::from(micros % 1_000) / 2_000.0)
}

/// A transport error without its URL, with the causes reqwest's own text leaves out.
pub(crate) fn transport_text(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut text = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(inner) = cause {
        text.push_str(&format!(": {inner}"));
        cause = inner.source();
    }
    text
}
