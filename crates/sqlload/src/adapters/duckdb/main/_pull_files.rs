//! Run a statement that has DuckDB read files itself, giving it access to the files' bucket first.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::_helpers::pull_sql::s3_secret;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::file_store::main::aws_variable::aws_variable;
use crate::file_store::main::s3_keys::s3_keys;
use crate::stage_api::models::AwsCredentials;
use crate::store_paths::models::StoreKind;

/// Run `insert` and return the rows it loaded. For S3 files it first loads `httpfs` (installing it on first use) and sets a [temporary secret](s3_secret) for the bucket, in the connection's region, else `AWS_REGION` / `AWS_DEFAULT_REGION` (unset when empty), else us-east-1 as SQLLoad's reader does: the keys SQLLoad's own S3 reads use (the `AWS_*` variables, else the shared credentials file under the connection's `profile` or `AWS_PROFILE`), passed as parameters and never in SQL, otherwise the AWS credential chain.
pub(in crate::adapters) async fn pull_files(
    destination: &DuckDbDestination,
    store: &StoreKind,
    insert: String,
) -> Result<u64, DuckDbError> {
    let (bucket, keys) = match store {
        StoreKind::S3 {
            bucket,
            region,
            profile,
            ..
        } => (
            Some((bucket.clone(), region.clone())),
            s3_keys(profile.as_deref())
                .map_err(DuckDbError::Task)?
                .map(secret_parameters),
        ),
        StoreKind::Filesystem { .. } | StoreKind::Sftp { .. } => (None, None),
    };
    let region = aws_variable("AWS_REGION")
        .or_else(|| aws_variable("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_owned());
    with_connection(destination, move |connection| {
        if let Some((bucket, bucket_region)) = bucket {
            connection.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
            if keys.is_none() {
                connection.execute_batch("INSTALL aws; LOAD aws;")?;
            }
            let (sql, parameters) =
                s3_secret(&bucket, bucket_region.unwrap_or(region), keys.as_ref());
            connection.execute(&sql, duckdb::params_from_iter(parameters))?;
        }
        Ok(connection.execute(&insert, [])? as u64)
    })
    .await
}

/// `keys` as the `KEY_ID`, `SECRET` and `SESSION_TOKEN` parameters of a DuckDB secret.
fn secret_parameters(keys: AwsCredentials) -> [String; 3] {
    let token = keys.session_token.map(|token| token.0).unwrap_or_default();
    [keys.key_id, keys.secret.0, token]
}
