//! Run SQL statements on the caller's session.

use crate::adapters::clickhouse::main::session::_run_session_statements::run_session_statements;
use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT, ROLLBACK_STATEMENT};
use crate::adapters::session::_helpers::blocking::on_session;
use crate::adapters::session::models::SessionDestination;
use crate::column_types::models::Warehouse;
use crate::loader::errors::LoaderError;

/// Run each statement in order on the destination's session, stopping at the first failure; a transaction open then (a failed `COMMIT` included) is rolled back, so a failed write leaves the table as it was. The caller's session never ends with the statements, as SQLLoad's own Snowflake requests do, so SQLLoad rolls back itself. A ClickHouse session's statements, whose publish steps SQLLoad carries out itself, run as on SQLLoad's own ClickHouse connection.
pub(in crate::adapters) async fn run_statements(
    destination: &SessionDestination,
    statements: Vec<String>,
) -> Result<(), LoaderError> {
    if statements.is_empty() {
        return Ok(());
    }
    if destination.connection.warehouse == Warehouse::ClickHouse {
        return run_session_statements(destination, &statements).await;
    }
    on_session(destination, move |session| {
        let mut in_transaction = false;
        for statement in &statements {
            tracing::debug!(%statement, "running on the caller's session");
            let opens = statement.trim() == BEGIN_STATEMENT;
            if let Err(error) = session.execute(statement) {
                if (in_transaction || opens)
                    && let Err(rollback) = session.execute(ROLLBACK_STATEMENT)
                {
                    tracing::warn!(error = %rollback, "cannot roll back after a failed statement");
                }
                return Err(error);
            }
            in_transaction = match statement.trim() {
                BEGIN_STATEMENT => true,
                COMMIT_STATEMENT | ROLLBACK_STATEMENT => false,
                _ => in_transaction,
            };
        }
        Ok(())
    })
    .await
}
