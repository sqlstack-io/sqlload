//! Commit a load's files: record what they replace, then finish replacing it.

use bytes::Bytes;

use crate::adapters::files::_helpers::layout::markers::{
    decided_folder, marker_text, pending_folder,
};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{PendingPhase, PendingRun};
use crate::file_store::main::create_file::create_file;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::store_paths::models::Location;

/// Decide commit `file_id` `replacing`, recording the files it replaces (`keys`) and a logged table's run record, which commits its files; write the run record, delete the replaced files, then its markers, so a stop in between is finished by the next repair. The decision is created only if absent: if another load repaired the table after this load's lease lapsed (the caller checked it just before, but may have stalled since), it decided the commit `aborted` and deleted its files, so this fails without deleting anything.
pub async fn commit_replaced(
    folder: &OpenFolder,
    file_id: &str,
    (keys, run): (&[String], Option<&PendingRun>),
) -> Result<(), FilesDestinationError> {
    let decided = decided_folder(folder);
    let text = Bytes::from(marker_text(PendingPhase::Replacing, keys, run));
    if !create_file(&decided, file_id, text).await? {
        return Err(FilesDestinationError::Write {
            key: format!("{}{file_id}", decided.location),
            reason: "this commit was already decided, by another load that repaired this table after this load's lease lapsed (or by this load's own retried write); stopped before deleting any files".to_owned(),
        });
    }
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
    let id = [file_id.to_owned()];
    delete_files(&pending_folder(folder), &id).await?;
    Ok(delete_files(&decided, &id).await?)
}
