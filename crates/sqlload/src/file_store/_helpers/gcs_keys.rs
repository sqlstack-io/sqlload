//! The GCS client SQLLoad reads a bucket with, and how it signs in: the connection's key, else Google's application default credentials, through the same sign-in as BigQuery.

use std::sync::Arc;

use object_store::ClientOptions;
use object_store::CredentialProvider;
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};

use crate::auth::models::Secret;
use crate::file_store::constants::GCS_SCOPE;
use crate::google_identity::main::google_access_token::google_access_token;
use crate::google_identity::main::google_credentials::google_credentials;
use crate::google_identity::models::GoogleCredentials;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;

/// Cloud Storage tokens for Google's application default credentials, renewed before they expire.
#[derive(Debug)]
struct GcsTokens {
    http: reqwest::Client,
    credentials: GoogleCredentials,
    kept: tokio::sync::Mutex<Option<(Secret, i64)>>,
}

#[async_trait::async_trait]
impl CredentialProvider for GcsTokens {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        let token =
            google_access_token(&self.http, &self.credentials, &self.kept, (GCS_SCOPE, None))
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "GCS",
                    source: Box::new(error),
                })?;
        Ok(Arc::new(GcpCredential {
            bearer: token.expose().to_owned(),
        }))
    }
}

/// How a GCS client signs in, for `sqlload debug` (never the credentials themselves), with `variable` reading the environment: the connection's key when it `has_key`, else Google's application default credentials as BigQuery finds them; the error when they cannot be used.
pub(crate) fn gcs_credentials(
    has_key: bool,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<String, String> {
    match has_key {
        true => Ok("the service-account key in the connection's service_account_key".to_owned()),
        false => google_credentials(None, None, variable).map(|found| found.describe()),
    }
}

/// The GCS client builder for `bucket`, signed in with the service-account key's JSON text `key`, else Google's application default credentials as [`google_credentials`] finds them with `variable` (the file `GOOGLE_APPLICATION_CREDENTIALS` names, of any type BigQuery takes; gcloud's; else the machine's service account); the error when that variable names no file or one SQLLoad cannot sign in with, rather than signing in as someone else. `endpoint` replaces Google's, as for an emulator.
pub(crate) fn gcs_builder(
    bucket: &str,
    key: Option<&str>,
    endpoint: Option<&str>,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<GoogleCloudStorageBuilder, String> {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    builder = match key {
        Some(key) => builder.with_service_account_key(key),
        None => builder.with_credentials(Arc::new(GcsTokens {
            http: build_http_client(&ConnectionCounter::default())
                .map_err(|error| error.to_string())?,
            credentials: google_credentials(None, None, variable)?,
            kept: tokio::sync::Mutex::default(),
        })),
    };
    if let Some(endpoint) = endpoint {
        builder = builder.with_base_url(endpoint).with_client_options(
            ClientOptions::new().with_allow_http(endpoint.starts_with("http://")),
        );
    }
    Ok(builder)
}
