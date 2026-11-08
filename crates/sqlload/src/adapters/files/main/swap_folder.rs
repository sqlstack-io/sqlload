//! Swap a table staged beside its local folder into place.

use std::collections::BTreeSet;

use crate::adapters::files::_helpers::layout::swap::{carry_over, local_folder, swap_in};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::models::OpenFolder;

/// Put the table written in `staged` in place of `live`: first carry over every file of `live` the load does not replace (its run log, and any file beside the data), then swap the two folders in one rename, so a reader sees the old table or the new one, never both. The old table is then deleted, best effort: a failure warns, and the next load deletes it.
pub async fn swap_folder(
    live: &OpenFolder,
    staged: &OpenFolder,
    replaced: &BTreeSet<String>,
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
    tokio::task::spawn_blocking(move || swap_in(&staged_path, &live_path))
        .await
        .map_err(|error| failed(&live.location.prefix, error.to_string()))?
        .map_err(|error| {
            failed(
                &live.location.prefix,
                format!("cannot swap the new table in: {error}"),
            )
        })
}
