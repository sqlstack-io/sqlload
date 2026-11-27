//! A Sheets API client for a connection's service-account key, or the credentials file `GOOGLE_APPLICATION_CREDENTIALS` names.

use crate::auth::models::Secret;
use crate::google_identity::main::environment_credentials::environment_credentials;
use crate::http::errors::ApiError;
use crate::sheets_source::_helpers::keys::key_text;
use crate::sheets_source::constants::{BASE_URL, CREDENTIALS_VARIABLE, SERVICE_ACCOUNT_TYPE};
use crate::sheets_source::models::{KeySource, SheetsClient, SheetsCredential};

/// A client reading as the service account of the key `source` gives, or as whoever the file `GOOGLE_APPLICATION_CREDENTIALS` names signs in (as BigQuery reads it), against `base_url` (the public API when `None`); nothing is sent until the first read.
pub fn sheets_client(
    http: reqwest::Client,
    source: &KeySource,
    base_url: Option<&str>,
) -> Result<SheetsClient, ApiError> {
    let credentials = match source {
        KeySource::Environment => {
            if std::env::var(CREDENTIALS_VARIABLE)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(ApiError::Auth(format!(
                    "no service-account key: set service_account_file, service_account_json or service_account_json_base64 on the connection, or {CREDENTIALS_VARIABLE}"
                )));
            }
            environment_credentials(None, None).map_err(ApiError::Auth)?
        }
        _ => {
            let text = key_text(source)?;
            let parsed: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
                ApiError::Auth(format!(
                    "the service-account key is not a key file's JSON: {error}"
                ))
            })?;
            if let Some(kind) = parsed["type"]
                .as_str()
                .filter(|kind| *kind != SERVICE_ACCOUNT_TYPE)
            {
                return Err(ApiError::Auth(format!(
                    "the key is a {kind:?} credential; a google_sheets connection's key is a service-account key, and other credentials files go in the file {CREDENTIALS_VARIABLE} names"
                )));
            }
            environment_credentials(Some(&Secret(text)), None).map_err(ApiError::Auth)?
        }
    };
    Ok(SheetsClient {
        http,
        base_url: base_url
            .unwrap_or(BASE_URL)
            .trim_end_matches('/')
            .to_owned(),
        credential: SheetsCredential::Google(Box::new(credentials)),
        token: std::sync::Arc::default(),
    })
}
