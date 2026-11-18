//! Give DuckDB, or MotherDuck reading on its servers, access to a bucket of files.

use std::time::Instant;

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::_helpers::pull_sql::{bucket_access, secret_current};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::store_paths::models::StoreKind;

/// Set [`bucket_access`] for files in `store` on `destination`'s database, unless the bucket's secret [is current](secret_current): for every table's connection at once, as tables setting one at the same time conflict. Call it before the table opens its transaction, never inside one, so the secret commits for every connection and a rollback can't undo it. On MotherDuck the secret is the session's own, which its servers read the files with, and nothing is stored in the account.
pub(in crate::adapters) async fn open_bucket(
    destination: &DuckDbDestination,
    store: &StoreKind,
) -> Result<(), DuckDbError> {
    let Some(access) = bucket_access(store)? else {
        return Ok(());
    };
    let now = Instant::now();
    let wanted = access.set_at(now);
    let mut set = destination.secrets.lock().await;
    if secret_current(set.get(&access.bucket), &wanted, now) {
        return Ok(());
    }
    set.remove(&access.bucket);
    match &destination.connection {
        DuckDbConnection::Bridge(bridge) => {
            run_statement(bridge, access.extensions.to_owned(), Vec::new()).await?;
            run_statement(bridge, access.statement, access.parameters).await?;
        }
        DuckDbConnection::Local(_) => {
            let (extensions, statement, values) =
                (access.extensions, access.statement, access.parameters);
            with_connection(destination, move |connection| {
                connection.execute_batch(extensions)?;
                connection.execute(&statement, duckdb::params_from_iter(values))?;
                Ok(())
            })
            .await?;
        }
    }
    set.insert(access.bucket, wanted);
    Ok(())
}
