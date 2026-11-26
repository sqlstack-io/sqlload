//! One table's write session through the caller's session.

use crate::adapters::session::models::{SessionDestination, SessionWriter};
use crate::loader::errors::LoaderError;

/// The writer of one table's batches on `destination` (the table's own session), with a new folder for the files it writes.
pub(in crate::adapters) fn open_writer(
    destination: &SessionDestination,
) -> Result<SessionWriter, LoaderError> {
    let folder = tempfile::Builder::new()
        .prefix("sqlload-session-")
        .tempdir()
        .map_err(|error| {
            LoaderError::Invalid(format!(
                "cannot create a folder for the files a caller's session loads: {error}"
            ))
        })?;
    Ok(SessionWriter {
        destination: destination.clone(),
        staged: Vec::new(),
        written: 0,
        folder,
    })
}
