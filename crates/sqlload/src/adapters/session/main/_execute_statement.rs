//! Run one statement on the caller's session.

use crate::adapters::session::_helpers::blocking::on_session;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;

/// Run `statement` on the destination's session, in autocommit.
pub(in crate::adapters) async fn execute_statement(
    destination: &SessionDestination,
    statement: String,
) -> Result<(), LoaderError> {
    tracing::debug!(%statement, "running on the caller's session");
    on_session(destination, move |session| session.execute(&statement)).await
}
