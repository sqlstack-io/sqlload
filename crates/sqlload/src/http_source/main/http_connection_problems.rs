//! Check the request settings of an `http` connection when the project is read, before any request.

use crate::http_source::_helpers::endpoint::interval;
use crate::http_source::_helpers::oauth::token_url;
use crate::http_source::constants::{MAX_IN_FLIGHT, MAX_RETRIES};
use crate::http_source::models::{Auth, ConnectionLimits};

/// Why the `auth`, `rate_limit`, `max_in_flight` and `retries` of the `http` connection `name` cannot be used, if they cannot.
pub fn http_connection_problems(
    name: &str,
    auth: Option<&Auth>,
    (limits, retries): (&ConnectionLimits, Option<u32>),
) -> Vec<String> {
    let place = format!("[connections.{name}]");
    let mut problems: Vec<String> = Vec::new();
    problems.extend(match auth {
        Some(
            Auth::Oauth2ClientCredentials(spec)
            | Auth::Oauth2RefreshToken(spec)
            | Auth::Oauth2Password(spec),
        ) => token_url(spec).err(),
        Some(Auth::ApiKey { header, query }) if header.is_some() == query.is_some() => {
            Some("auth api_key takes exactly one of header or query".to_owned())
        }
        None | Some(Auth::Bearer {} | Auth::ApiKey { .. } | Auth::Basic {}) => None,
    });
    problems.extend(
        limits
            .rate_limit
            .as_deref()
            .and_then(|rate| interval(rate).err()),
    );
    if limits
        .max_in_flight
        .is_some_and(|most| !(1..=MAX_IN_FLIGHT).contains(&most))
    {
        problems.push(format!("max_in_flight must be from 1 to {MAX_IN_FLIGHT}"));
    }
    if retries.is_some_and(|retries| retries > MAX_RETRIES) {
        problems.push(format!("retries must be at most {MAX_RETRIES}"));
    }
    problems
        .into_iter()
        .map(|problem| format!("{place} {problem}"))
        .collect()
}
