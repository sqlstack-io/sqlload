//! The GCS client SQLLoad reads a bucket with.

use object_store::ClientOptions;
use object_store::gcp::GoogleCloudStorageBuilder;

/// The GCS client builder for `bucket`, signed in with the service-account key's JSON text `key`, else Google's application default credentials (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud user's, else the instance's service account); `endpoint` replaces Google's, as for an emulator.
pub fn gcs_builder(
    bucket: &str,
    key: Option<&str>,
    endpoint: Option<&str>,
) -> GoogleCloudStorageBuilder {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    if let Some(key) = key {
        builder = builder.with_service_account_key(key);
    }
    if let Some(endpoint) = endpoint {
        builder = builder.with_base_url(endpoint).with_client_options(
            ClientOptions::new().with_allow_http(endpoint.starts_with("http://")),
        );
    }
    builder
}
