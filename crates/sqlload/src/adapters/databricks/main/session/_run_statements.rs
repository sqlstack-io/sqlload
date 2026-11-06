//! Run SQL statements in order.

use crate::adapters::databricks::constants::TRANSACTION_STATEMENTS;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::http::errors::ApiError;

/// Run `statements` one after another, each committing on its own; a transaction statement is refused, since the Statement Execution API cannot hold one open across requests.
pub(in crate::adapters) async fn run_statements(
    destination: &DatabricksDestination,
    statements: &[String],
) -> Result<(), ApiError> {
    if let Some(statement) = statements.iter().find(|statement| {
        TRANSACTION_STATEMENTS.contains(&statement.trim().to_ascii_uppercase().as_str())
    }) {
        return Err(ApiError::Protocol(format!(
            "{statement} was sent to Databricks, which runs every statement in its own transaction"
        )));
    }
    for statement in statements {
        execute_statement(&destination.api, statement).await?;
    }
    Ok(())
}
