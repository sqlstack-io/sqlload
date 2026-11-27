//! Find how a Google connection signs in, in Google's order.

use crate::auth::models::Secret;
use crate::google_identity::_helpers::files::{credentials_file, impersonation, key_fields};
use crate::google_identity::constants::{
    CREDENTIALS_VARIABLE, IAM_CREDENTIALS_URL, METADATA_HOST, METADATA_HOST_VARIABLE,
};
use crate::google_identity::models::{CredentialOrigin, GoogleCredentials, Signer};

/// How a connection signs in, with `variable` reading the environment: its `service_account_key` when it sets one (a key file's JSON text); else the credentials file `GOOGLE_APPLICATION_CREDENTIALS` names, which must exist; else gcloud's application default credentials file (`~/.config/gcloud/application_default_credentials.json`, or `%APPDATA%\gcloud\…` on Windows) when it exists; else the machine's service account, from its metadata server. With `impersonate`, the token of whichever signs in gets tokens for that service account. A file of a type SQLLoad cannot sign in with is an error naming it, never a reason to try the next.
pub fn google_credentials(
    key: Option<&Secret>,
    impersonate: Option<&str>,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<GoogleCredentials, String> {
    let (origin, signer, from_file) = match key {
        Some(key) => {
            let what = "the connection's service_account_key";
            let parsed: serde_json::Value = serde_json::from_str(key.expose())
                .map_err(|error| format!("{what} is not a key file's JSON: {error}"))?;
            match parsed["type"].as_str() {
                None | Some("service_account") => (
                    CredentialOrigin::Connection,
                    Signer::ServiceAccount(key_fields(&parsed, what)?),
                    None,
                ),
                Some(other) => {
                    return Err(format!(
                        "{what} is a {other:?} credentials file; it takes a service-account key, and other credentials files go in the file {CREDENTIALS_VARIABLE} names"
                    ));
                }
            }
        }
        None => match variable(CREDENTIALS_VARIABLE) {
            Some(path) => {
                let text = std::fs::read_to_string(&path).map_err(|error| {
                    format!("{CREDENTIALS_VARIABLE} names {path}, which cannot be read: {error}")
                })?;
                let (signer, file_impersonates) =
                    credentials_file(&text, &format!("{CREDENTIALS_VARIABLE} ({path})"), variable)?;
                (CredentialOrigin::Named { path }, signer, file_impersonates)
            }
            None => match gcloud_file(variable) {
                Some(path) => {
                    let text = std::fs::read_to_string(&path).map_err(|error| {
                        format!("gcloud's application default credentials {path} cannot be read: {error}")
                    })?;
                    let (signer, file_impersonates) = credentials_file(
                        &text,
                        &format!("gcloud's application default credentials ({path})"),
                        variable,
                    )?;
                    (CredentialOrigin::Gcloud { path }, signer, file_impersonates)
                }
                None => (
                    CredentialOrigin::Machine,
                    Signer::Metadata {
                        host: variable(METADATA_HOST_VARIABLE)
                            .unwrap_or_else(|| METADATA_HOST.to_owned()),
                    },
                    None,
                ),
            },
        },
    };
    let impersonate = match (from_file, impersonate) {
        (Some(_), Some(account)) => {
            return Err(format!(
                "the connection's impersonate_service_account ({account}) and its credentials file both name a service account to impersonate; keep one"
            ));
        }
        (Some(found), None) => Some(found),
        (None, Some(account)) => Some(impersonation(
            &format!(
                "{IAM_CREDENTIALS_URL}/projects/-/serviceAccounts/{account}:generateAccessToken"
            ),
            Vec::new(),
            None,
            "impersonate_service_account",
        )?),
        (None, None) => None,
    };
    Ok(GoogleCredentials {
        origin,
        signer,
        impersonate,
    })
}

/// gcloud's application default credentials file, when it exists.
fn gcloud_file(variable: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let (home, below) = match cfg!(windows) {
        true => ("APPDATA", "gcloud/application_default_credentials.json"),
        false => (
            "HOME",
            ".config/gcloud/application_default_credentials.json",
        ),
    };
    variable(home)
        .map(|home| std::path::Path::new(&home).join(below))
        .filter(|path| path.is_file())
        .map(|path| path.display().to_string())
}
