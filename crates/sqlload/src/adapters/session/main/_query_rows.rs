//! Run a query on the caller's session.

use crate::adapters::session::_helpers::blocking::on_session;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;

/// The rows `query` gives on the destination's session, each value as text.
pub(in crate::adapters) async fn query_rows(
    destination: &SessionDestination,
    query: String,
) -> Result<Vec<Vec<Option<String>>>, LoaderError> {
    tracing::debug!(%query, "querying the caller's session");
    on_session(destination, move |session| session.query_text(&query)).await
}
