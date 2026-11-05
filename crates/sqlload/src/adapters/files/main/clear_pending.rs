//! Record that a load finished.

use crate::adapters::files::_helpers::layout::markers::pending_folder;
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::models::OpenFolder;

/// Delete the load's pending marker.
pub async fn clear_pending(
    folder: &OpenFolder,
    load_id: &str,
) -> Result<(), FilesDestinationError> {
    Ok(delete_files(&pending_folder(folder), &[load_id.to_owned()]).await?)
}
