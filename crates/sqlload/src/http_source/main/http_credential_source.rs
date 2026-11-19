//! Say where an `http` connection's credentials come from, as `sqlload debug` checks them.

use crate::http_source::models::Auth;

/// `auth bearer: token from the connection`, or for OAuth2 also the token URL its access tokens come from; an error naming each setting the connection's `auth` needs and lacks. `set` lists which credential settings the connection has, by name.
pub fn http_credential_source(auth: Option<&Auth>, set: &[(&str, bool)]) -> Result<String, String> {
    let (kind, needed, token_url): (&str, &[&str], Option<&str>) = match auth {
        None => return Ok("no auth".to_owned()),
        Some(Auth::Bearer {}) => ("bearer", &["token"], None),
        Some(Auth::ApiKey { .. }) => ("api_key", &["token"], None),
        Some(Auth::Basic {}) => ("basic", &["username", "password"], None),
        Some(Auth::Oauth2ClientCredentials(spec)) => (
            "oauth2_client_credentials",
            &["client_id", "client_secret"],
            Some(&spec.token_url),
        ),
        Some(Auth::Oauth2RefreshToken(spec)) => (
            "oauth2_refresh_token",
            &["refresh_token"],
            Some(&spec.token_url),
        ),
        Some(Auth::Oauth2Password(spec)) => (
            "oauth2_password",
            &["username", "password"],
            Some(&spec.token_url),
        ),
    };
    let missing: Vec<&str> = needed
        .iter()
        .copied()
        .filter(|key| !set.contains(&(*key, true)))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "auth {kind} needs {} on the connection",
            missing.join(" and ")
        ));
    }
    let tokens = token_url.map_or_else(String::new, |url| format!("; access tokens from {url}"));
    Ok(format!(
        "auth {kind}: {} from the connection{tokens}",
        needed.join(" and ")
    ))
}
