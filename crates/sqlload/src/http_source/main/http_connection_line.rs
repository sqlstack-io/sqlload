//! Describe the request settings of each `http` connection, as `plan` shows them.

use std::collections::BTreeMap;

use crate::http_source::constants::DEFAULT_RETRIES;
use crate::http_source::models::Auth;
use crate::project::models::ConnectionConfig;

/// One line per `http` connection, such as `http  shop  rate_limit 10/s, max_in_flight 4, retries 5, auth bearer`: what every request to it shares in a run, how often a failed one is retried, and how it authenticates, never its credentials.
pub fn http_connection_lines(connections: &BTreeMap<String, ConnectionConfig>) -> Vec<String> {
    connections
        .iter()
        .filter_map(|(name, connection)| match connection {
            ConnectionConfig::Http {
                auth,
                rate_limit,
                max_in_flight,
                retries,
                ..
            } => Some(format!(
                "http  {name}  rate_limit {}, max_in_flight {}, retries {}, auth {}",
                rate_limit.as_deref().unwrap_or("none"),
                max_in_flight.map_or_else(|| "none".to_owned(), |most| most.to_string()),
                retries.unwrap_or(DEFAULT_RETRIES),
                auth.as_ref().map_or_else(|| "none".to_owned(), auth_scheme),
            )),
            _ => None,
        })
        .collect()
}

/// The connection's `auth` as `plan` names it: `bearer`, `api_key in header X-Api-Key`, `oauth2_password`, ...
fn auth_scheme(auth: &Auth) -> String {
    match auth {
        Auth::Bearer {} => "bearer".to_owned(),
        Auth::ApiKey {
            header: Some(header),
            ..
        } => format!("api_key in header {header}"),
        Auth::ApiKey { query, .. } => {
            format!("api_key in query {}", query.as_deref().unwrap_or_default())
        }
        Auth::Basic {} => "basic".to_owned(),
        Auth::Oauth2ClientCredentials(_) => "oauth2_client_credentials".to_owned(),
        Auth::Oauth2RefreshToken(_) => "oauth2_refresh_token".to_owned(),
        Auth::Oauth2Password(_) => "oauth2_password".to_owned(),
    }
}
