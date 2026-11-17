//! Swap a table staged beside its local folder into place.

use std::collections::BTreeSet;

use crate::adapters::files::_helpers::layout::swap::{carry_over, local_folder, swap_in};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::models::OpenFolder;
use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::models::Lease;

/// Put the table written in `staged` in place of `live`: carry over every file of `live` the load does not replace (its run log, its markers, any file beside the data); check the load still holds `lease` on `live`, then swap the folder `held` is open on in, in one rename, so a reader sees the old table or the new one, never both, checking it is that folder before and after the rename and undoing the rename if not (see `swap_in`): a load whose lease lapsed may have moved `staged` away, and this load's later writes made it again without the earlier files. Nothing is written between the lease check and the rename. `Ok(Err(why))` when the load lost the table's lease or another load moved its folder: the table is left as it was, and a rerun may pass. The old table is then deleted, best effort: a failure warns, and the next load deletes it.
pub async fn swap_folder(
    (live, lease): (&OpenFolder, &Lease),
    staged: &OpenFolder,
    (replaced, held): (&BTreeSet<String>, &std::fs::File),
) -> Result<Result<(), String>, FilesDestinationError> {
    let failed = |key: &str, reason: String| FilesDestinationError::Write {
        key: key.to_owned(),
        reason,
    };
    let (Some((live_path, _, _)), Some((staged_path, _, _))) =
        (local_folder(&live.location), local_folder(&staged.location))
    else {
        return Err(failed(
            &live.location.to_string(),
            "only a table folder below a local directory can be swapped".to_owned(),
        ));
    };
    carry_over(live, staged, replaced)
        .await
        .map_err(|(key, reason)| failed(&key, reason))?;
    let kept = keep_lease(live, lease, lease_now())
        .await
        .map_err(|error| failed(&live.location.to_string(), error.to_string()))?;
    if let Err(lost) = kept {
        return Ok(Err(format!("{lost}; the new table was not swapped in")));
    }
    let held = held
        .try_clone()
        .map_err(|error| failed(&staged.location.to_string(), error.to_string()))?;
    tokio::task::spawn_blocking(move || swap_in((&staged_path, &live_path), &held, |_| ()))
        .await
        .map_err(|error| failed(&live.location.prefix, error.to_string()))?
        .map_err(|error| {
            failed(
                &live.location.prefix,
                format!("cannot swap the new table in: {error}"),
            )
        })
}
