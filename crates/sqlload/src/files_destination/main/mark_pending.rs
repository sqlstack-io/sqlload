//! Record that a load is under way.

use bytes::Bytes;

use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_destination::_helpers::markers::{marker_text, pending_folder};
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::models::{PendingPhase, PendingRun};

/// Put (or replace) the load's marker `_sqlload/pending/<load id>` with its phase and, when replacing, the keys it deletes and the run record it writes.
pub async fn mark_pending(
    folder: &OpenFolder,
    load_id: &str,
    phase: PendingPhase,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), FilesDestinationError> {
    let text = marker_text(phase, keys, run);
    Ok(write_file(&pending_folder(folder), load_id, Bytes::from(text)).await?)
}
