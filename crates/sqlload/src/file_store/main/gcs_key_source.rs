//! Where a GCS connection's credentials come from, for `sqlload debug`.

use crate::file_store::_helpers::aws_keys::environment_variable;
use crate::file_store::_helpers::gcs_keys::gcs_credentials;

/// Where a GCS client's credentials come from (never the credentials themselves) when its connection `has_key` or not, or why they cannot be used.
pub fn gcs_key_source(has_key: bool) -> Result<String, String> {
    gcs_credentials(has_key, &environment_variable).map(|source| source.describe())
}
