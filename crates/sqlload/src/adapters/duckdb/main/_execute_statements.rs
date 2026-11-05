//! Run SQL statements in order.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;

/// Run each statement in order, stopping at the first failure (an open transaction is rolled back).
pub(in crate::adapters) async fn execute_statements(
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
