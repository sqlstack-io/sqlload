//! The GCS client SQLLoad reads a bucket with.

use object_store::ClientOptions;
use object_store::gcp::GoogleCloudStorageBuilder;

use crate::file_store::_helpers::aws_keys::environment_variable;
use crate::file_store::_helpers::gcs_keys::{GcsCredentials, gcs_credentials};

/// The GCS client builder for `bucket`, signed in with the service-account key's JSON text `key`, else Google's application default credentials (the file `GOOGLE_APPLICATION_CREDENTIALS` names, gcloud's, else the machine's service account); `endpoint` replaces Google's, as for an emulator.
pub fn gcs_builder(
    bucket: &str,
    key: Option<&str>,
    endpoint: Option<&str>,
) -> GoogleCloudStorageBuilder {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    match (key, gcs_credentials(key.is_some(), &environment_variable)) {
        (Some(key), _) => builder = builder.with_service_account_key(key),
        (None, Ok(GcsCredentials::Named { path })) => {
            builder = builder.with_application_credentials(path);
        }
        (None, _) => {}
    }
    if let Some(endpoint) = endpoint {
        builder = builder.with_base_url(endpoint).with_client_options(
            ClientOptions::new().with_allow_http(endpoint.starts_with("http://")),
        );
    }
    builder
}
