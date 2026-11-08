//! The S3 client SQLLoad reads a bucket with.

use object_store::aws::AmazonS3Builder;

/// The S3 client builder for `bucket`, with credentials from `AWS_*` variables, web identity or the instance/container role, as SQLLoad reads S3 with.
pub fn s3_builder(bucket: &str, region: Option<&str>, endpoint: Option<&str>) -> AmazonS3Builder {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(endpoint.starts_with("http://"))
            .with_virtual_hosted_style_request(false);
    }
    builder
}
