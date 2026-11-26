//! Calls into the caller's session, which block, made from a blocking thread.

use std::sync::Arc;

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::session::errors::SessionError;
use crate::adapters::session::models::SessionDestination;
use crate::adapters::session::types::CallerSession;
use crate::loader::errors::LoaderError;

/// The destination's session, opened on first use.
pub(crate) async fn opened(
    destination: &SessionDestination,
) -> Result<Arc<dyn CallerSession>, LoaderError> {
    let caller = destination.connection.caller.clone();
    let dialect = sql_adapter(destination.connection.warehouse);
    destination
        .session
        .get_or_try_init(|| async move {
            tokio::task::spawn_blocking(move || prepared(caller.open()?, dialect))
                .await
                .map_err(joined)?
        })
        .await
        .cloned()
}

/// `session` set up as SQLLoad's own sessions are, once its autocommit is checked: with it off, statements outside SQLLoad's transactions would never commit.
fn prepared(
    session: Arc<dyn CallerSession>,
    dialect: &'static dyn SqlAdapter,
) -> Result<Arc<dyn CallerSession>, LoaderError> {
    if let Some(query) = dialect.render_autocommit_check() {
        let rows = session.query_text(&query)?;
        let value = rows.first().and_then(|row| row.get(1)).cloned().flatten();
        if value.is_some_and(|value| value.eq_ignore_ascii_case("false")) {
            return Err(LoaderError::Invalid(format!(
                "the caller's {} session has autocommit off; SQLLoad needs it on, and sends BEGIN and COMMIT itself",
                dialect.warehouse()
            )));
        }
    }
    for statement in dialect.render_session_setup() {
        session.execute(&statement)?;
    }
    Ok(session)
}

/// Run `work` on the destination's session from a blocking thread.
pub(crate) async fn on_session<T: Send + 'static>(
    destination: &SessionDestination,
    work: impl FnOnce(&dyn CallerSession) -> Result<T, SessionError> + Send + 'static,
) -> Result<T, LoaderError> {
    let session = opened(destination).await?;
    tokio::task::spawn_blocking(move || work(session.as_ref()))
        .await
        .map_err(joined)?
        .map_err(LoaderError::from)
}

fn joined(error: tokio::task::JoinError) -> LoaderError {
    LoaderError::Invalid(format!("the caller's session thread failed: {error}"))
}
