//! A service-account key's text, from where a connection says it is.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::google_identity::models::{GoogleCredentials, Signer};
use crate::http::errors::ApiError;
use crate::sheets_source::constants::CREDENTIALS_VARIABLE;
use crate::sheets_source::models::KeySource;

/// `credentials`, when they can ask for a token with the Sheets scope: workload identity federation can only by impersonating a service account, since Google STS's own tokens don't reach the Sheets API.
pub(crate) fn sheets_readable(
    credentials: GoogleCredentials,
) -> Result<GoogleCredentials, ApiError> {
    match (&credentials.signer, &credentials.impersonate) {
        (Signer::External(_), None) => Err(ApiError::Auth(format!(
            "{} reads Google Sheets only by impersonating a service account: give the file a service_account_impersonation_url (gcloud iam workload-identity-pools create-cred-config --service-account=…)",
            credentials.describe()
        ))),
        _ => Ok(credentials),
    }
}

/// The key file's JSON text that `source` gives.
pub(crate) fn key_text(source: &KeySource) -> Result<String, ApiError> {
    Ok(match source {
        KeySource::Json(json) => json.expose().to_owned(),
        KeySource::Base64(encoded) => {
            let bytes = STANDARD.decode(encoded.expose().trim()).map_err(|error| {
                ApiError::Auth(format!(
                    "service_account_json_base64 is not base64: {error}"
                ))
            })?;
            String::from_utf8(bytes).map_err(|_| {
                ApiError::Auth("service_account_json_base64 does not decode to text".to_owned())
            })?
        }
        KeySource::File(path) => read_key_file(path)?,
        KeySource::Environment => {
            return Err(ApiError::Protocol(format!(
                "the file {CREDENTIALS_VARIABLE} names is read as Google credentials, not as a key's text"
            )));
        }
    })
}

/// A key file's text.
fn read_key_file(path: &std::path::Path) -> Result<String, ApiError> {
    std::fs::read_to_string(path).map_err(|error| {
        ApiError::Auth(format!(
            "cannot read the service-account key file {}: {error}",
            path.display()
        ))
    })
}
