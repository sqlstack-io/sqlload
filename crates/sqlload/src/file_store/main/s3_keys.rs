//! The static keys SQLLoad's S3 clients and the warehouses it hands keys to use.

use crate::file_store::_helpers::aws_keys::{KeySource, environment_variable, key_source};
use crate::stage_api::models::AwsCredentials;

/// Keys from `AWS_*` variables, else from the shared credentials file under `profile` (else `AWS_PROFILE`, else `default`); `None` leaves the rest of the chain (web identity, the instance or container role) to the client; or why a named profile cannot be used.
pub fn s3_keys(profile: Option<&str>) -> Result<Option<AwsCredentials>, String> {
    let variable = environment_variable;
    Ok(match key_source(profile, &variable)? {
        KeySource::Environment => Some(AwsCredentials {
            key_id: variable("AWS_ACCESS_KEY_ID").unwrap_or_default(),
            secret: crate::auth::models::Secret(
                variable("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
            ),
            session_token: variable("AWS_SESSION_TOKEN").map(crate::auth::models::Secret),
        }),
        KeySource::File { keys, .. } => Some(keys),
        KeySource::Chain => None,
    })
}
