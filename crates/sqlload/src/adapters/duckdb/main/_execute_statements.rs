//! Run SQL statements in order.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::_run_statement::run_statement;

/// Run each statement in order, stopping at the first failure (an open transaction is rolled back).
pub(in crate::adapters) async fn execute_statements(
    destination: &DuckDbDestination,
    statements: Vec<String>,
) -> Result<(), DuckDbError> {
    if let DuckDbConnection::Bridge(bridge) = &destination.connection {
        for statement in statements {
            if let Err(error) = run_statement(bridge, statement, Vec::new()).await {
                let _ = run_statement(bridge, "ROLLBACK".to_owned(), Vec::new()).await;
                return Err(error.into());
            }
        }
        return Ok(());
    }
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
