//! A Sheets API client for a connection's service-account key.

use crate::bigquery_api::main::parse_key::parse_key;
use crate::http::errors::ApiError;
use crate::sheets_source::_helpers::keys::key_text;
use crate::sheets_source::constants::{BASE_URL, SERVICE_ACCOUNT_TYPE};
use crate::sheets_source::models::{KeySource, SheetsClient, SheetsCredential};

/// A client reading as the service account of the key `source` gives, against `base_url` (the public API when `None`); nothing is sent until the first read.
pub fn sheets_client(
    http: reqwest::Client,
    source: &KeySource,
    base_url: Option<&str>,
) -> Result<SheetsClient, ApiError> {
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
            "the key is a {kind:?} credential; Google Sheets sources read with a service-account key"
        )));
    }
    Ok(SheetsClient {
        http,
        base_url: base_url
            .unwrap_or(BASE_URL)
            .trim_end_matches('/')
            .to_owned(),
        credential: SheetsCredential::ServiceAccount(parse_key(&text)?),
        token: std::sync::Arc::default(),
    })
}
