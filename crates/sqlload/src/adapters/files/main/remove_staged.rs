//! Delete the staging folders a table's earlier loads left behind.

use crate::adapters::files::_helpers::layout::swap::{local_folder, staged_stem};
use crate::store_paths::models::Location;

/// Delete every hidden folder beside table folder `live` that a load of it staged in: one a stopped load left, or the old table a load swapped out but could not delete. Best effort: a failure only warns, and the next load tries again.
pub async fn remove_staged(live: &Location) {
    let Some((path, _, name)) = local_folder(live) else {
        return;
    };
    let (Some(parent), stem) = (
        path.parent().map(std::path::Path::to_path_buf),
        staged_stem(name),
    ) else {
        return;
    };
    let removed = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let entries = match std::fs::read_dir(&parent) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with(&stem) {
                std::fs::remove_dir_all(entry.path())?;
            }
        }
        Ok(())
    })
    .await;
    if let Err(error) = removed
        .map_err(|error| error.to_string())
        .and_then(|removed| removed.map_err(|error| error.to_string()))
    {
        tracing::warn!(%error, folder = %live, "cannot delete a staging folder an earlier load left beside the table; the next load tries again");
    }
}
