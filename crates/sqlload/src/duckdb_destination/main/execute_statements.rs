//! Run SQL statements in order.

use crate::duckdb_destination::_helpers::blocking::with_connection;
use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;

/// Run each statement in order, stopping at the first failure (an open transaction is rolled back).
pub async fn execute_statements(
    destination: &DuckDbDestination,
    statements: Vec<String>,
) -> Result<(), DuckDbError> {
    with_connection(destination, move |connection| {
        for statement in &statements {
            if let Err(error) = connection.execute_batch(statement) {
                let _ = connection.execute_batch("ROLLBACK");
                return Err(error.into());
            }
        }
        Ok(())
    })
    .await
}
