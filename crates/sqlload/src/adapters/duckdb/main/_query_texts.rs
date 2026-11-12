//! Run a query returning one text column.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::_run_statement::run_statement;

/// The first column of every row of `query`, as text; SQL `NULL`s are skipped.
pub(in crate::adapters) async fn query_texts(
    destination: &DuckDbDestination,
    query: String,
) -> Result<Vec<String>, DuckDbError> {
    if let DuckDbConnection::Bridge(bridge) = &destination.connection {
        let rows = run_statement(bridge, query, Vec::new()).await?;
        return Ok(rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect());
    }
    with_connection(destination, move |connection| {
        let mut statement = connection.prepare(&query)?;
        let rows = statement.query_map([], |row| row.get::<_, Option<String>>(0))?;
        let values: Vec<Option<String>> = rows.collect::<Result<_, _>>()?;
        Ok(values.into_iter().flatten().collect())
    })
    .await
}
