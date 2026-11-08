//! The S3 client SQLLoad reads a bucket with.

use object_store::aws::AmazonS3Builder;

use crate::file_store::_helpers::aws_keys::{KeySource, environment_variable, key_source};

/// The S3 client builder for `bucket`, with keys from `AWS_*` variables, else the shared credentials file under `profile` (else `AWS_PROFILE`), else web identity or the instance/container role; or why a named profile cannot be used.
pub fn s3_builder(
    bucket: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    profile: Option<&str>,
) -> Result<AmazonS3Builder, String> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let KeySource::File { keys, .. } = key_source(profile, &environment_variable)? {
        builder = builder
            .with_access_key_id(keys.key_id)
            .with_secret_access_key(keys.secret.0);
        if let Some(token) = keys.session_token {
            builder = builder.with_token(token.0);
        }
    }
    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(endpoint.starts_with("http://"))
            .with_virtual_hosted_style_request(false);
    }
    Ok(builder)
}
