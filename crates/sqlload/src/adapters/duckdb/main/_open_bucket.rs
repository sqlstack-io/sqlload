//! Give DuckDB, or MotherDuck reading on its servers, access to a bucket of files.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::_helpers::pull_sql::bucket_access;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::store_paths::models::StoreKind;

/// Set [`bucket_access`] for files in `store` on `destination`'s database, unless the same secret is already set: once per bucket for every table's connection, as tables setting one at the same time conflict, and again when its keys change. Call it before the table opens its transaction, so the secret commits for every connection. On MotherDuck the secret is the session's own, which its servers read the files with, and nothing is stored in the account.
pub(in crate::adapters) async fn open_bucket(
    destination: &DuckDbDestination,
    store: &StoreKind,
) -> Result<(), DuckDbError> {
    let Some((extensions, (secret, parameters))) = bucket_access(store)? else {
        return Ok(());
    };
    let mut set = destination.secrets.lock().await;
    if set.contains(&(secret.clone(), parameters.clone())) {
        return Ok(());
    }
    match &destination.connection {
        DuckDbConnection::Bridge(bridge) => {
            run_statement(bridge, extensions.to_owned(), Vec::new()).await?;
            run_statement(bridge, secret.clone(), parameters.clone()).await?;
        }
        DuckDbConnection::Local(_) => {
            let (statement, values) = (secret.clone(), parameters.clone());
            with_connection(destination, move |connection| {
                connection.execute_batch(extensions)?;
                connection.execute(&statement, duckdb::params_from_iter(values))?;
                Ok(())
            })
            .await?;
        }
    }
    set.retain(|(statement, _)| *statement != secret);
    set.push((secret, parameters));
    Ok(())
}
