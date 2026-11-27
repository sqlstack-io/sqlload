//! Find how a Google connection signs in, from the process environment.

use crate::auth::models::Secret;
use crate::google_identity::main::google_credentials::google_credentials;
use crate::google_identity::models::GoogleCredentials;

/// How a connection with the service-account `key` and `impersonate` signs in, as [`google_credentials`] finds it in the process environment (variables set but empty count as unset).
pub fn environment_credentials(
    key: Option<&Secret>,
    impersonate: Option<&str>,
) -> Result<GoogleCredentials, String> {
    let variable = |name: &str| match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) | Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    };
    google_credentials(key, impersonate, &variable)
}
