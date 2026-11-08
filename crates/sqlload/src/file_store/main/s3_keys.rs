//! The static keys SQLLoad's S3 clients and the warehouses it hands keys to use.

use crate::file_store::_helpers::aws_keys::{environment_variable, key_source};
use crate::stage_api::models::AwsCredentials;

/// Keys from `AWS_*` variables, else from the shared credentials file under `profile` (else `AWS_PROFILE`, else `default`); `None` leaves the rest of the chain (web identity, the instance or container role) to the client; or why a named profile cannot be used.
pub fn s3_keys(profile: Option<&str>) -> Result<Option<AwsCredentials>, String> {
    Ok(key_source(profile, &environment_variable)?.keys())
}
