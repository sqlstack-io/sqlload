//! Calls into the caller's session, which block, made from a blocking thread.

use std::sync::Arc;

use crate::adapters::session::errors::SessionError;
use crate::adapters::session::models::SessionDestination;
use crate::adapters::session::types::CallerSession;
use crate::loader::errors::LoaderError;

/// The destination's session, opened on first use.
pub(crate) async fn opened(
    destination: &SessionDestination,
) -> Result<Arc<dyn CallerSession>, LoaderError> {
    let caller = destination.connection.caller.clone();
    destination
        .session
        .get_or_try_init(|| async move {
            tokio::task::spawn_blocking(move || caller.open())
                .await
                .map_err(joined)?
                .map_err(LoaderError::from)
        })
        .await
        .cloned()
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
