//! Run a query returning one text column.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// The first column of every row of `query`, as text; SQL `NULL`s are skipped.
pub(in crate::adapters) async fn query_texts(
    destination: &DuckDbDestination,
    query: String,
) -> Result<Vec<String>, DuckDbError> {
    with_connection(destination, move |connection| {
        let mut statement = connection.prepare(&query)?;
        let rows = statement.query_map([], |row| row.get::<_, Option<String>>(0))?;
        let values: Vec<Option<String>> = rows.collect::<Result<_, _>>()?;
        Ok(values.into_iter().flatten().collect())
    })
    .await
}
