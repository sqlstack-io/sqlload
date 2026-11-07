//! Read a service-account key file.

use crate::auth::models::Secret;
use crate::bigquery_api::models::ServiceAccountKey;
use crate::http::errors::ApiError;

/// The key a service-account key file's JSON text holds; the private key is checked when the first token is signed.
pub fn parse_key(text: &str) -> Result<ServiceAccountKey, ApiError> {
    let parsed: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        ApiError::Auth(format!(
            "the service-account key is not a key file's JSON: {error}"
        ))
    })?;
    let field = |name: &str| -> Result<String, ApiError> {
        parsed[name]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| ApiError::Auth(format!("the service-account key has no {name}")))
    };
    Ok(ServiceAccountKey {
        client_email: field("client_email")?,
        private_key: Secret(field("private_key")?),
        token_uri: field("token_uri")?,
        project_id: field("project_id")?,
    })
}
