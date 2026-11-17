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
use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::models::Lease;
use bytes::Bytes;

/// For each unfinished load, delete its files (it stopped writing) or finish it (its files are complete): write the run record it holds, delete the files it replaces; then delete its marker. A load found writing may only have lost its lease and still be running, so repair first decides it `aborted` (created only if the load has not decided `replacing` meanwhile, in which case it is left to finish). The load may also have committed in full since the table was listed, deleting its decided marker: it deletes its pending marker first and never writes it again, so a pending marker gone once `aborted` is created means it committed, and repair withdraws the decision and leaves its files. A decision that holds keeps the pending marker beside it (so one left without it, by a repair that stopped while withdrawing it, is deleted alone); it has the late commit fail, and later repairs delete files the load still stores, until it expires. Then merge the table's live records (`records`, which never count the files repair deletes) once there are many. Before any of that, in a local directory, take away the hidden folders beside the table that other loads staged a table in, so none can be swapped in any more; then check this load still holds `lease`, extending it. A load that checked its own lease before this one took the table may have swapped its folder in after: that swap carried this load's lease file away with the old table, so the check fails, and the error says why, before anything is repaired. Otherwise any such swap came before this load took the table, so `pending` and `records`, read after that, are current.
pub async fn repair_table(
    (folder, lease): (&OpenFolder, &Lease),
    (pending, records): (&[PendingLoad], &LiveRecords),
) -> Result<Result<(), String>, FilesDestinationError> {
    remove_staged(&folder.location).await?;
    let kept = keep_lease(folder, lease, lease_now())
        .await
        .map_err(|error| FilesDestinationError::Write {
            key: folder.location.to_string(),
            reason: error.to_string(),
        })?;
    if let Err(lost) = kept {
        return Ok(Err(lost));
    }
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
    compact_records(folder, records).await?;
    Ok(Ok(()))
}
