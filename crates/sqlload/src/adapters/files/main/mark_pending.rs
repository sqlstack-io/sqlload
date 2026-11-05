//! Record that a load is under way.

use bytes::Bytes;

use crate::adapters::files::_helpers::layout::markers::{marker_text, pending_folder};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{PendingPhase, PendingRun};
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;

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
