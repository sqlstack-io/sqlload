//! Swap a table staged beside its local folder into place.

use std::collections::BTreeSet;

use crate::adapters::files::_helpers::layout::swap::{
    carry_over, check_held, local_folder, swap_in,
};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::models::OpenFolder;
use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::models::Lease;

/// Put the table written in `staged` in place of `live`: carry over every file of `live` the load does not replace (its run log, its markers, any file beside the data); check the load still holds `lease` on `live`, extending it, and that `staged` is still the folder `held` is open on (a load whose lease lapsed may have moved it away, and this load's later writes made it again without the earlier files); then swap the two folders in one rename, so a reader sees the old table or the new one, never both. Nothing is written between the checks and the rename, so a load that stalls in between past its lease, while another load takes the table and moves `staged` away, fails at the rename without changing the table. The old table is then deleted, best effort: a failure warns, and the next load deletes it.
pub async fn swap_folder(
    (live, lease): (&OpenFolder, &Lease),
    staged: &OpenFolder,
    (replaced, held): (&BTreeSet<String>, &std::fs::File),
) -> Result<(), FilesDestinationError> {
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
    keep_lease(live, lease, lease_now())
        .await
        .map_err(|error| failed(&live.location.to_string(), error.to_string()))?
        .map_err(|lost| {
            failed(
                &live.location.to_string(),
                format!("{lost}; the new table was not swapped in"),
            )
        })?;
    let held = held
        .try_clone()
        .map_err(|error| failed(&staged.location.to_string(), error.to_string()))?;
    tokio::task::spawn_blocking(move || {
        check_held(&held, &staged_path).and_then(|()| swap_in(&staged_path, &live_path))
    })
    .await
    .map_err(|error| failed(&live.location.prefix, error.to_string()))?
    .map_err(|error| {
        failed(
            &live.location.prefix,
            format!("cannot swap the new table in: {error}"),
        )
    })
}
