//! Run a ClickHouse write's statements on a caller's session.

use crate::adapters::clickhouse::main::session::_run_statements::run_statements;
use crate::adapters::clickhouse::models::SessionSteps;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;

/// Run `statements` on the caller's ClickHouse session as on SQLLoad's own connection: each publish step carried out, asking the session what the server answers, rather than sent as SQL.
pub(in crate::adapters) async fn run_session_statements(
    destination: &SessionDestination,
    statements: &[String],
) -> Result<(), LoaderError> {
    run_statements(&SessionSteps { destination }, statements).await
}
