//! A destination for one table's load.

use std::sync::Arc;

use crate::adapters::session::models::SessionDestination;

/// `destination` with a session of its own, opened on first use, so the table's temporary tables and transactions never mix with another table loading at the same time; schema changes still wait for each other.
pub(in crate::adapters) fn table_session(destination: &SessionDestination) -> SessionDestination {
    SessionDestination {
        session: Arc::default(),
        ..destination.clone()
    }
}
