//! Run SQL statements in order.

use crate::adapters::bigquery::constants::TRANSACTION_STATEMENTS;
use crate::adapters::bigquery::models::BigQueryDestination;
use crate::bigquery_api::main::run_query::run_query;
use crate::http::errors::ApiError;

/// Run `statements` one after another, each its own query job committing on its own; a transaction statement is refused, since separate jobs cannot hold one open.
pub(in crate::adapters) async fn run_statements(
    destination: &BigQueryDestination,
    statements: &[String],
) -> Result<(), ApiError> {
    if let Some(statement) = statements.iter().find(|statement| {
        TRANSACTION_STATEMENTS.contains(&statement.trim().to_ascii_uppercase().as_str())
    }) {
        return Err(ApiError::Protocol(format!(
            "{statement} was sent to BigQuery, which runs every statement as its own job"
        )));
    }
    for statement in statements {
        run_query(&destination.api, statement).await?;
    }
    Ok(())
}
