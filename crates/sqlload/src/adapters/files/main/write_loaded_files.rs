//! Record the source files a load from a files source loaded.

use bytes::Bytes;

use crate::adapters::files::_helpers::layout::loaded::{loaded_key, loaded_text};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::LoadedFile;

/// Put (or replace) load `load_id`'s record of the source files it loaded, each with the commit whose files hold its rows; an entry counts only while the table holds that commit's files, so a commit that never finished or whose files were replaced claims nothing.
pub async fn write_loaded_files(
    folder: &OpenFolder,
    load_id: &str,
    entries: &[(String, LoadedFile)],
) -> Result<(), FilesDestinationError> {
    let text = loaded_text(entries).map_err(|reason| FilesDestinationError::Write {
        key: format!("{}{}", folder.location, loaded_key(load_id)),
        reason,
    })?;
    Ok(write_file(folder, &loaded_key(load_id), Bytes::from(text)).await?)
}
