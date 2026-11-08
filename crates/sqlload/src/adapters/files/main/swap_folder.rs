//! Swap a table staged beside its folder into place.

use std::collections::BTreeSet;

use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;

use crate::adapters::files::_helpers::layout::swap::{local_folder, swap_dirs};
use crate::adapters::files::constants::PENDING_FOLDER;
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
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
    let live_prefix = live.location.prefix.as_str();
    let listed: Vec<object_store::ObjectMeta> = match live_path.try_exists() {
        Ok(true) => live
            .store
            .list(Some(&object_path(live_prefix).map_err(|source| {
                StoreError::Object {
                    action: "cannot list",
                    key: live.location.to_string(),
                    source,
                }
            })?))
            .try_collect()
            .await
            .map_err(|source| StoreError::Object {
                action: "cannot list",
                key: live.location.to_string(),
                source,
            })?,
        _ => Vec::new(),
    };
    for meta in listed {
        let Some(key) = meta.location.as_ref().strip_prefix(live_prefix) else {
            continue;
        };
        if replaced.contains(key) || key.starts_with(PENDING_FOLDER) {
            continue;
        }
        let to = object_path(&format!("{}{key}", staged.location.prefix))
            .map_err(|error| failed(key, error.to_string()))?;
        match staged.store.copy_if_not_exists(&meta.location, &to).await {
            Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(failed(key, format!("cannot carry over: {error}"))),
        }
    }
    let (from, to) = (staged_path.clone(), live_path.clone());
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&from)?;
        swap_dirs(&from, &to)
    })
    .await
    .map_err(|error| failed(live_prefix, error.to_string()))?
    .map_err(|error| {
        failed(
            live_prefix,
            format!("cannot swap the new table in: {error}"),
        )
    })?;
    let removed =
        tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&staged_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            removed => removed,
        })
        .await;
    if let Err(error) = removed
        .map_err(|error| error.to_string())
        .and_then(|removed| removed.map_err(|error| error.to_string()))
    {
        tracing::warn!(%error, folder = %live.location, "the new table is in place, but the old one beside it could not be deleted; the next load deletes it");
    }
    Ok(())
}
