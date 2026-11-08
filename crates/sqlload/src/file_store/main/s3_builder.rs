//! The S3 client SQLLoad reads a bucket with.

use object_store::aws::AmazonS3Builder;

use crate::file_store::_helpers::aws_keys::{environment_variable, key_source, keyed_builder};

/// The S3 client builder for `bucket`, with keys from `AWS_*` variables, else the shared credentials file under `profile` (else `AWS_PROFILE`), else web identity or the instance/container role; or why a named profile cannot be used.
pub fn s3_builder(
    bucket: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    profile: Option<&str>,
) -> Result<AmazonS3Builder, String> {
    let keys = key_source(profile, &environment_variable)?.keys();
    let variables = std::env::vars_os().filter_map(|(name, value)| {
        match (name.into_string(), value.into_string()) {
            (Ok(name), Ok(value)) => Some((name, value)),
            _ => None,
        }
    });
    let mut builder = keyed_builder(variables, keys).with_bucket_name(bucket);
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
