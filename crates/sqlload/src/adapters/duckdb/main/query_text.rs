//! Run a query returning one text value.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

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
