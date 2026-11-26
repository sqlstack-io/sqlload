//! What a session destination connects as.

use crate::adapters::session::_helpers::blocking::opened;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;

/// The line a destination check prints for the caller's connection, once its session opens.
pub(in crate::adapters) async fn check_session(
    destination: &SessionDestination,
) -> Result<String, LoaderError> {
    opened(destination).await?;
    Ok(format!(
        "{} through the caller's connection: {}",
        destination.connection.warehouse,
        destination.connection.caller.describe()
    ))
}
