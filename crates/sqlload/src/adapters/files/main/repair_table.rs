//! Repair the loads a table's pending markers record.

use crate::adapters::files::_helpers::layout::loaded::compact_records;
use crate::adapters::files::_helpers::layout::markers::{marker_text, pending_folder};
use crate::adapters::files::_helpers::layout::swap::remove_staged;
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{LiveRecords, PendingLoad, PendingPhase};
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::store_paths::models::Location;
use bytes::Bytes;

/// For each unfinished load, delete its files (it stopped writing) or finish it (its files are complete): write the run record it holds, delete the files it replaces; then delete its marker. A load found writing may only have lost its lease and still be storing files, so its marker becomes `aborted` instead, which has later repairs delete those files too until it expires. Then merge the table's live records (`records`, which never count the files repair deletes) once there are many. In a local directory, first delete the hidden folders beside the table that earlier loads staged in, putting back a table a stopped swap left moved aside.
pub async fn repair_table(
    folder: &OpenFolder,
    (pending, records): (&[PendingLoad], &LiveRecords),
) -> Result<(), FilesDestinationError> {
    remove_staged(&folder.location).await;
    let markers = pending_folder(folder);
    for load in pending {
        if let (PendingPhase::Replacing, Some(run)) = (load.phase, &load.run) {
            let records = OpenFolder {
                store: folder.store.clone(),
                location: Location {
                    store: folder.location.store.clone(),
                    prefix: run.prefix.clone(),
                },
            };
            write_file(&records, &run.name, Bytes::from(run.json.clone())).await?;
        }
        delete_files(folder, &load.deletes).await?;
        match (load.phase, load.expired) {
            (PendingPhase::Writing, _) => {
                let aborted = marker_text(PendingPhase::Aborted, &[], None);
                write_file(&markers, &load.load_id, Bytes::from(aborted)).await?;
            }
            (PendingPhase::Aborted, false) => {}
            _ => delete_files(&markers, std::slice::from_ref(&load.load_id)).await?,
        }
    }
    compact_records(folder, records).await
}
