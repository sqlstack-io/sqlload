//! Run SQL statements in order.

use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::models::PostgresDestination;
use crate::loader::errors::LoaderError;

/// Run each statement in order on the target's session, stopping at the first failure; an open transaction is rolled back, so a failed write leaves the table as it was.
pub(in crate::adapters) async fn run_statements(
    destination: &PostgresDestination,
    statements: &[String],
) -> Result<(), LoaderError> {
    if statements.is_empty() {
        return Ok(());
    }
    let held = session(destination).await?;
    let Some((client, _)) = held.as_ref() else {
        return Ok(());
    };
    for statement in statements {
        tracing::debug!(%statement, "running on Postgres");
        if let Err(error) = client.batch_execute(statement).await {
            if !client.is_closed()
                && let Err(rollback) = client.batch_execute("ROLLBACK").await
            {
                tracing::warn!(error = %rollback, "cannot roll back after a failed statement");
            }
            return Err(query_error(error));
        }
    }
    Ok(())
}
