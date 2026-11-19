//! Check the request settings of an `http` connection when the project is read, before any request.

use crate::http_source::_helpers::endpoint::interval;
use crate::http_source::_helpers::oauth::token_url;
use crate::http_source::models::{Auth, ConnectionLimits};

/// Why the `auth`, `rate_limit` and `max_in_flight` of the `http` connection `name` cannot be used, if they cannot.
pub fn http_connection_problems(name: &str, auth: &Auth, limits: &ConnectionLimits) -> Vec<String> {
    let place = format!("[connections.{name}]");
    let mut problems: Vec<String> = Vec::new();
    problems.extend(match auth {
        Auth::Oauth2ClientCredentials(spec)
        | Auth::Oauth2RefreshToken(spec)
        | Auth::Oauth2Password(spec) => token_url(spec).err(),
        Auth::ApiKey { header, query } if header.is_some() == query.is_some() => {
            Some("auth api_key takes exactly one of header or query".to_owned())
        }
        Auth::None | Auth::Bearer | Auth::ApiKey { .. } | Auth::Basic => None,
    });
    problems.extend(
        limits
            .rate_limit
            .as_deref()
            .and_then(|rate| interval(rate).err()),
    );
    if limits.max_in_flight == Some(0) {
        problems.push("max_in_flight must be at least 1".to_owned());
    }
    problems
        .into_iter()
        .map(|problem| format!("{place} {problem}"))
        .collect()
}
