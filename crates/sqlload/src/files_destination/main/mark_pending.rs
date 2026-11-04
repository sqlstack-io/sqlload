//! Record that a load is under way.

use bytes::Bytes;

use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_destination::_helpers::markers::{marker_text, pending_folder};
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::models::PendingPhase;

/// Put (or replace) the load's marker `_sqlload/pending/<load id>` with its phase and, when replacing, the keys it deletes.
pub async fn mark_pending(
    folder: &OpenFolder,
    load_id: &str,
    phase: PendingPhase,
    keys: &[String],
) -> Result<(), FilesDestinationError> {
    let text = marker_text(phase, keys);
    Ok(write_file(&pending_folder(folder), load_id, Bytes::from(text)).await?)
}
