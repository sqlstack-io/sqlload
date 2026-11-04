//! Repair the loads a table's pending markers record.

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::OpenFolder;
use crate::files_destination::_helpers::markers::pending_folder;
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::models::PendingLoad;

/// For each unfinished load, delete its files (it stopped writing) or the files it replaces (it stopped deleting them), then its marker.
pub async fn repair_table(
    folder: &OpenFolder,
    pending: &[PendingLoad],
) -> Result<(), FilesDestinationError> {
    let markers = pending_folder(folder);
    for load in pending {
        delete_files(folder, &load.deletes).await?;
        delete_files(&markers, std::slice::from_ref(&load.load_id)).await?;
    }
    Ok(())
}
