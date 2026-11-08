//! Where an S3 connection's keys come from, for `sqlload debug`.

use crate::file_store::_helpers::aws_keys::{environment_variable, key_source};

/// Where an S3 client's keys under `profile` come from (never the keys themselves), or why that profile cannot be used.
pub fn s3_key_source(profile: Option<&str>) -> Result<String, String> {
    key_source(profile, &environment_variable).map(|source| source.describe())
}
