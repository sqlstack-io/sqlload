//! What gives DuckDB, or MotherDuck reading on its servers, access to a bucket of files.

use crate::adapters::duckdb::_helpers::motherduck_connection::environment_variable;
use crate::adapters::duckdb::_helpers::pull_sql::{default_region, s3_secret};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::file_store::main::s3_keys::s3_keys;
use crate::stage_api::models::AwsCredentials;
use crate::store_paths::models::StoreKind;

/// For files in S3, the extensions to load (installing them on first use) and the [temporary secret](s3_secret), with its parameters, for the bucket: in the connection's region, else `AWS_REGION` / `AWS_DEFAULT_REGION` (unset when empty), else us-east-1 as SQLLoad's reader does; with the keys SQLLoad's own S3 reads use (the `AWS_*` variables, else the shared credentials file under the connection's `profile` or `AWS_PROFILE`), passed as parameters and never in SQL, otherwise the AWS credential chain, resolved on this machine. `None` for files elsewhere.
pub(crate) fn bucket_access(
    store: &StoreKind,
) -> Result<Option<(&'static str, (String, Vec<String>))>, DuckDbError> {
    let StoreKind::S3 {
        bucket,
        region,
        profile,
        ..
    } = store
    else {
        return Ok(None);
    };
    let keys = s3_keys(profile.as_deref())
        .map_err(DuckDbError::Task)?
        .map(secret_parameters);
    let extensions = match keys {
        Some(_) => "INSTALL httpfs; LOAD httpfs;",
        None => "INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;",
    };
    let region = region
        .clone()
        .unwrap_or_else(|| default_region(&environment_variable));
    Ok(Some((extensions, s3_secret(bucket, region, keys.as_ref()))))
}

/// `keys` as the `KEY_ID`, `SECRET` and `SESSION_TOKEN` parameters of a DuckDB secret.
fn secret_parameters(keys: AwsCredentials) -> [String; 3] {
    let token = keys.session_token.map(|token| token.0).unwrap_or_default();
    [keys.key_id, keys.secret.0, token]
}
