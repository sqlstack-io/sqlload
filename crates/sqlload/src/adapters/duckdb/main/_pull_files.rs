//! Run a statement that has DuckDB, or MotherDuck on its servers, read files itself.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;

/// Run `insert`, which reads files from a bucket [opened](crate::adapters::duckdb::main::_open_bucket::open_bucket) before the table's transaction, and return the rows it loaded.
pub(in crate::adapters) async fn pull_files(
    destination: &DuckDbDestination,
    insert: String,
) -> Result<u64, DuckDbError> {
    let DuckDbConnection::Bridge(bridge) = &destination.connection else {
        return with_connection(destination, move |connection| {
            Ok(connection.execute(&insert, [])? as u64)
        })
        .await;
    };
    let rows = run_statement(bridge, insert, Vec::new()).await?;
    let Some(count) = rows.first().and_then(|row| row.first()).cloned().flatten() else {
        return Err(DuckDbError::MotherDuck(
            "the statement reported no row count".to_owned(),
        ));
    };
    count.parse::<u64>().map_err(|error| {
        DuckDbError::MotherDuck(format!("the statement reported {count:?} rows: {error}"))
    })
}
