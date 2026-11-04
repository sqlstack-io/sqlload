//! Record that a load finished.

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::OpenFolder;
use crate::files_destination::_helpers::markers::pending_folder;
use crate::files_destination::errors::FilesDestinationError;

/// Delete the load's pending marker.
pub async fn clear_pending(
    folder: &OpenFolder,
    load_id: &str,
) -> Result<(), FilesDestinationError> {
    Ok(delete_files(&pending_folder(folder), &[load_id.to_owned()]).await?)
}
