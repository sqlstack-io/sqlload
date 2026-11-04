//! Run a query returning one text value.

use crate::duckdb_destination::_helpers::blocking::with_connection;
use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;

/// The first column of the first row of `query`, as text; `None` for SQL `NULL`.
pub async fn query_text(
    destination: &DuckDbDestination,
    query: String,
) -> Result<Option<String>, DuckDbError> {
    with_connection(destination, move |connection| {
        Ok(connection.query_row(&query, [], |row| row.get::<_, Option<String>>(0))?)
    })
    .await
}
