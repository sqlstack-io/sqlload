//! Run a statement that has DuckDB, or MotherDuck on its servers, read files itself, giving it access to the files' bucket first.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::_helpers::s3_access::bucket_access;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::store_paths::models::StoreKind;

/// Run `insert` and return the rows it loaded, after [`bucket_access`] for S3 files. On MotherDuck the secret is the bridge session's own, which MotherDuck's servers read the files with, and nothing is stored in the account.
pub(in crate::adapters) async fn pull_files(
    destination: &DuckDbDestination,
    store: &StoreKind,
    insert: String,
) -> Result<u64, DuckDbError> {
    let access = bucket_access(store)?;
    let DuckDbConnection::Bridge(bridge) = &destination.connection else {
        return with_connection(destination, move |connection| {
            if let Some((extensions, (secret, parameters))) = access {
                connection.execute_batch(extensions)?;
                connection.execute(&secret, duckdb::params_from_iter(parameters))?;
            }
            Ok(connection.execute(&insert, [])? as u64)
        })
        .await;
    };
    if let Some((extensions, (secret, parameters))) = access {
        run_statement(bridge, extensions.to_owned(), Vec::new()).await?;
        run_statement(bridge, secret, parameters).await?;
    }
    let rows = run_statement(bridge, insert, Vec::new()).await?;
    let count = rows.first().and_then(|row| row.first()).cloned().flatten();
    count
        .as_deref()
        .and_then(|count| count.parse::<u64>().ok())
        .ok_or_else(|| {
            DuckDbError::MotherDuck(format!(
                "the insert reported {count:?} rows rather than a count"
            ))
        })
}
