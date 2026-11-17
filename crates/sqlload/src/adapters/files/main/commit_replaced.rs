//! Commit a load's files: record what they replace, then finish replacing it.

use bytes::Bytes;

use crate::adapters::files::_helpers::layout::markers::pending_folder;
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::main::mark_pending::mark_pending;
use crate::adapters::files::models::{PendingPhase, PendingRun};
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::store_paths::models::Location;

/// Record commit `file_id`'s replaced files (`keys`) and a logged table's run record in its marker, which commits its files; write the run record, delete the replaced files, then the marker, so a stop in between is finished by the next repair. The caller has just checked that it still holds the table's lease.
pub async fn commit_replaced(
    folder: &OpenFolder,
    file_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), FilesDestinationError> {
    mark_pending(folder, file_id, PendingPhase::Replacing, (keys, run)).await?;
    if let Some(run) = run {
        let records = OpenFolder {
            store: folder.store.clone(),
            location: Location {
                store: folder.location.store.clone(),
                prefix: run.prefix.clone(),
            },
        };
        write_file(&records, &run.name, Bytes::from(run.json.clone())).await?;
    }
    delete_files(folder, keys).await?;
    Ok(delete_files(&pending_folder(folder), &[file_id.to_owned()]).await?)
}
