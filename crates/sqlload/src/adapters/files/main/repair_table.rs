//! Repair the loads a table's pending markers record.

use crate::adapters::files::_helpers::layout::loaded::compact_records;
use crate::adapters::files::_helpers::layout::markers::{
    decided_folder, marker_text, pending_folder,
};
use crate::adapters::files::_helpers::layout::swap::remove_staged;
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{LiveRecords, PendingLoad, PendingPhase};
use crate::file_store::errors::StoreError;
use crate::file_store::main::create_file::create_file;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::store_paths::models::Location;
use bytes::Bytes;

/// For each unfinished load, delete its files (it stopped writing) or finish it (its files are complete): write the run record it holds, delete the files it replaces; then delete its marker. A load found writing may only have lost its lease and still be running, so repair first decides it `aborted` (created only if the load has not decided `replacing` meanwhile, in which case it is left to finish). The load may also have committed in full since the table was listed, deleting its decided marker: it deletes its pending marker first and never writes it again, so a pending marker gone once `aborted` is created means it committed, and repair withdraws the decision and leaves its files. A decision that holds keeps the pending marker beside it (so one left without it, by a repair that stopped while withdrawing it, is deleted alone); it has the late commit fail, and later repairs delete files the load still stores, until it expires. Then merge the table's live records (`records`, which never count the files repair deletes) once there are many. In a local directory, first delete the hidden folders beside the table that earlier loads staged in, putting back a table a stopped swap left moved aside.
pub async fn repair_table(
    folder: &OpenFolder,
    (pending, records): (&[PendingLoad], &LiveRecords),
) -> Result<(), FilesDestinationError> {
    remove_staged(&folder.location).await;
    let (markers, decided) = (pending_folder(folder), decided_folder(folder));
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
        let id = std::slice::from_ref(&load.load_id);
        match (load.phase, load.expired) {
            (PendingPhase::Writing, _) => {
                let aborted = Bytes::from(marker_text(PendingPhase::Aborted, &[], None));
                if !create_file(&decided, &load.load_id, aborted).await? {
                    continue;
                }
                match read_file(&markers, &load.load_id).await {
                    Err(StoreError::Object {
                        source: object_store::Error::NotFound { .. },
                        ..
                    }) => {
                        delete_files(&decided, id).await?;
                        continue;
                    }
                    found => found?,
                };
                delete_files(folder, &load.deletes).await?;
            }
            (PendingPhase::Aborted, false) => delete_files(folder, &load.deletes).await?,
            _ => {
                delete_files(folder, &load.deletes).await?;
                delete_files(&markers, id).await?;
                delete_files(&decided, id).await?;
            }
        }
    }
    compact_records(folder, records).await
}
