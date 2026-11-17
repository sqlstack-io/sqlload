//! The hidden folder a filesystem `replace` stages a table in, beside its folder, and the rename that swaps the two.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

use crate::adapters::files::constants::{LOAD_ID_LENGTH, MOVED_ASIDE_SUFFIX, STAGED_FOLDER_MARK};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::types::Exchange;
use crate::file_store::models::OpenFolder;
use crate::store_paths::models::{Location, StoreKind};

/// A table folder on a local directory: its path, its parent's prefix and its name; `None` in other stores and for the directory's root.
pub(crate) fn local_folder(location: &Location) -> Option<(PathBuf, &str, &str)> {
    let StoreKind::Filesystem { root } = &location.store else {
        return None;
    };
    let trimmed = location.prefix.trim_end_matches('/');
    let (parent, name) = match trimmed.rsplit_once('/') {
        Some((parent, name)) => (&location.prefix[..=parent.len()], name),
        None => ("", trimmed),
    };
    (!name.is_empty()).then(|| (Path::new(root).join(trimmed), parent, name))
}

/// The start of the name of every folder loads of table folder `name` stage in.
pub(crate) fn staged_stem(name: &str) -> String {
    format!(".{name}{STAGED_FOLDER_MARK}")
}

/// Put `staged` at `live` and `live`'s old contents at `staged`, in one `renameat2(RENAME_EXCHANGE)` (`renameatx_np(RENAME_SWAP)` on macOS), so a reader sees one folder or the other. Neither is created: when `staged` is gone because a load that took the table after this load's lease lapsed moved it away, this fails and changes nothing. A missing `live` is renamed into place. Where the system or filesystem has no such rename this fails too, rather than move `live` aside in steps another load could interleave with.
pub(crate) fn swap_dirs(staged: &Path, live: &Path) -> std::io::Result<()> {
    match live.try_exists()? {
        false => std::fs::rename(staged, live),
        true => exchange(staged, live).map_err(|error| match error.kind() == std::io::ErrorKind::Unsupported {
            true => std::io::Error::other(format!(
                "this filesystem swapped two folders in one rename when the load started, but no longer can ({error}); stopped before changing the table"
            )),
            false => error,
        }),
    }
}

/// Whether `entry` is a folder a load of the table named by `stem` staged in, `<stem><load id>`, and whether it is one an earlier release moved an old table aside to, `<stem><load id>-old`.
pub(crate) fn staged_entry(entry: &str, stem: &str) -> Option<bool> {
    let rest = entry.strip_prefix(stem)?;
    let (load_id, aside) = match rest.strip_suffix(MOVED_ASIDE_SUFFIX) {
        Some(load_id) => (load_id, true),
        None => (rest, false),
    };
    let is_load_id = load_id.len() == LOAD_ID_LENGTH
        && load_id
            .chars()
            .all(|character| character.is_ascii_hexdigit() || character == '-');
    is_load_id.then_some(aside)
}

/// A directory entry that is a staging folder of the table named by `stem`, and whether it is an old table moved aside.
fn staged_dir(entry: &std::fs::DirEntry, stem: &str) -> Option<(PathBuf, bool)> {
    staged_entry(&entry.file_name().to_string_lossy(), stem).map(|aside| (entry.path(), aside))
}

/// Swap two existing directories in one rename; `Unsupported` where the system or filesystem has no such rename.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn exchange(staged: &Path, live: &Path) -> std::io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    use rustix::io::Errno;
    renameat_with(CWD, staged, CWD, live, RenameFlags::EXCHANGE).map_err(|errno| {
        match [Errno::INVAL, Errno::NOSYS, Errno::OPNOTSUPP, Errno::NOTSUP].contains(&errno) {
            true => std::io::ErrorKind::Unsupported.into(),
            false => errno.into(),
        }
    })
}

/// Swap two existing directories in one rename; `Unsupported` where the system or filesystem has no such rename.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn exchange(_staged: &Path, _live: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// Copy into `staged` every file of `live` the load does not replace: by hard link, or by copying the bytes where the filesystem has no hard links. Returns the key that failed and why.
pub(crate) async fn carry_over(
    live: &OpenFolder,
    staged: &OpenFolder,
    replaced: &BTreeSet<String>,
) -> Result<(), (String, String)> {
    let live_prefix = live.location.prefix.as_str();
    let exists = local_folder(&live.location).is_some_and(|(path, _, _)| path.exists());
    let listed: Vec<object_store::ObjectMeta> = match exists {
        true => {
            let prefix = ObjectPath::parse(live_prefix.trim_end_matches('/'))
                .map_err(|error| (live_prefix.to_owned(), error.to_string()))?;
            live.store
                .list(Some(&prefix))
                .try_collect()
                .await
                .map_err(|error| (live_prefix.to_owned(), format!("cannot list: {error}")))?
        }
        false => Vec::new(),
    };
    for meta in listed {
        let Some(key) = kept_key(&meta.location, live_prefix, replaced) else {
            continue;
        };
        let to = ObjectPath::parse(format!("{}{key}", staged.location.prefix))
            .map_err(|error| (key.clone(), error.to_string()))?;
        copy_file(live, staged, (&meta.location, &to))
            .await
            .map_err(|error| (key, format!("cannot carry over: {error}")))?;
    }
    Ok(())
}

/// The key below the table folder of a listed file the swap keeps: one the load doesn't replace. Markers come too: a tombstone is a decided marker and the pending marker beside it.
fn kept_key(location: &ObjectPath, prefix: &str, replaced: &BTreeSet<String>) -> Option<String> {
    let key = location.as_ref().strip_prefix(prefix)?;
    (!replaced.contains(key)).then(|| key.to_owned())
}

/// Copy one file by hard link, or by its bytes where that fails; one already there is kept.
async fn copy_file(
    live: &OpenFolder,
    staged: &OpenFolder,
    (from, to): (&ObjectPath, &ObjectPath),
) -> Result<(), object_store::Error> {
    match staged.store.copy_if_not_exists(from, to).await {
        Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(_) => {
            let bytes = live.store.get(from).await?.bytes().await?;
            staged.store.put(to, bytes.into()).await.map(|_| ())
        }
    }
}

/// Swap the staged folder in, then delete the old table now at `staged`; a failed delete only warns.
pub(crate) fn swap_in(staged: &Path, live: &Path) -> std::io::Result<()> {
    swap_dirs(staged, live)?;
    let removed = match std::fs::remove_dir_all(staged) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        removed => removed,
    };
    if let Err(error) = removed {
        tracing::warn!(%error, folder = %live.display(), "the new table is in place, but the old one beside it could not be deleted; the next load deletes it");
    }
    Ok(())
}

/// Move every staging folder of table folder `live` (named by `stem`) in its parent away to a fresh name, each in one rename, then delete them: the renames fail the call, a failed delete only warns (the folder keeps a staging name no load owns, so the next load deletes it). An old table an earlier release moved aside is kept, with a warning: a stop at the wrong moment left it the table's only copy.
pub(crate) fn clear_staged(live: &Path, stem: &str) -> std::io::Result<()> {
    let Some(parent) = live.parent() else {
        return Ok(());
    };
    let entries: Vec<(PathBuf, bool)> = match std::fs::read_dir(parent) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter_map(|entry| staged_dir(&entry, stem))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let mut moved: Vec<PathBuf> = Vec::new();
    for (entry, aside) in entries {
        if aside {
            tracing::warn!(folder = %entry.display(), table = %live.display(), "an earlier SQLLoad moved this table's old files aside here and stopped before finishing its swap; it is kept so you can check whether it holds rows the table lacks, then delete it");
            continue;
        }
        let away = parent.join(format!("{stem}{}", uuid::Uuid::new_v4()));
        match std::fs::rename(&entry, &away) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            renamed => {
                renamed?;
                moved.push(away);
            }
        }
    }
    for away in moved {
        if let Err(error) = std::fs::remove_dir_all(&away) {
            tracing::warn!(%error, folder = %away.display(), "cannot delete a staging folder beside the table; the next load deletes it");
        }
    }
    Ok(())
}

/// Before a load that holds the table's lease reads it: take away every hidden folder beside table folder `live` that another load staged a new table in, so that load can't swap it in any more. Each goes in one rename, then is deleted. A load whose lease lapsed may still be about to swap: either its swap came first, and what is moved away is the old table it swapped out, or this rename did, and its swap fails. A rename that fails stops this load before it writes anything.
pub(crate) async fn remove_staged(live: &Location) -> Result<(), FilesDestinationError> {
    let Some((path, _, name)) = local_folder(live) else {
        return Ok(());
    };
    let stem = staged_stem(name);
    tokio::task::spawn_blocking(move || clear_staged(&path, &stem))
        .await
        .map_err(|error| error.to_string())
        .and_then(|removed| removed.map_err(|error| error.to_string()))
        .map_err(|reason| FilesDestinationError::Write {
            key: live.to_string(),
            reason: format!(
                "cannot move away a staging folder an earlier load left beside the table: {reason}"
            ),
        })
}

/// Whether `exchange` swaps two folders in one rename (its support) beside table folder `live` (named by `stem`): tried on two empty folders created there, named like staging folders so the next load deletes any a stop leaves; `false` only where it fails with `Unsupported`.
pub(crate) fn exchange_support(
    live: &Path,
    stem: &str,
    exchange: Exchange,
) -> std::io::Result<bool> {
    let Some(parent) = live.parent() else {
        return Ok(false);
    };
    std::fs::create_dir_all(parent)?;
    let probes = [
        parent.join(format!("{stem}{}", uuid::Uuid::new_v4())),
        parent.join(format!("{stem}{}", uuid::Uuid::new_v4())),
    ];
    probes.iter().try_for_each(std::fs::create_dir)?;
    let swapped = exchange(&probes[0], &probes[1]);
    for probe in &probes {
        if let Err(error) = std::fs::remove_dir(probe) {
            tracing::warn!(%error, folder = %probe.display(), "cannot delete an empty folder made to check the filesystem; the next load deletes it");
        }
    }
    match swapped {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => Ok(false),
        Err(error) => Err(error),
    }
}
