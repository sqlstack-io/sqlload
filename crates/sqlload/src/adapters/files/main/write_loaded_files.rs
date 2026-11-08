//! Record the source files a load from a files source loaded.

use bytes::Bytes;

use crate::adapters::files::_helpers::layout::loaded::{loaded_key, loaded_text};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::LoadedFile;

/// Put commit `file_id`'s record of the source files whose rows its files hold; it counts only while the table holds that commit's files, so a commit that never finished or whose files were replaced claims nothing.
pub async fn write_loaded_files(
    folder: &OpenFolder,
    file_id: &str,
    files: &[LoadedFile],
) -> Result<(), FilesDestinationError> {
    let key = loaded_key(file_id);
    let text = loaded_text(files).map_err(|reason| FilesDestinationError::Write {
        key: format!("{}{key}", folder.location),
        reason,
    })?;
    Ok(write_file(folder, &key, Bytes::from(text)).await?)
}
