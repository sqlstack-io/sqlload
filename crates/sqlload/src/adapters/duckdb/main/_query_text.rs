//! Run a query returning one text value.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::_run_statement::run_statement;

/// The first column of the first row of `query`, as text; `None` for SQL `NULL`.
pub(in crate::adapters) async fn query_text(
    destination: &DuckDbDestination,
    query: String,
) -> Result<Option<String>, DuckDbError> {
    if let DuckDbConnection::Bridge(bridge) = &destination.connection {
        let rows = run_statement(bridge, query, Vec::new()).await?;
        let first = rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next());
        return first
            .ok_or_else(|| DuckDbError::MotherDuck("the query returned no rows".to_owned()));
    }
    with_connection(destination, move |connection| {
        Ok(connection.query_row(&query, [], |row| row.get::<_, Option<String>>(0))?)
    })
    .await
}
