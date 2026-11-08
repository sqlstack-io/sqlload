//! The hidden folder a filesystem `replace` stages a table in, beside its folder, and the rename that swaps the two.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

use crate::adapters::files::constants::{
    LOAD_ID_LENGTH, MOVED_ASIDE_SUFFIX, PENDING_FOLDER, STAGED_FOLDER_MARK,
};
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

/// Put `staged` at `live` and `live`'s old contents at `staged`. With an existing `live` this is one `renameat2(RENAME_EXCHANGE)` (`renameatx_np(RENAME_SWAP)` on macOS), so a reader sees one folder or the other; where the system or filesystem has no swap, `live` is moved aside first, leaving a moment with no folder.
pub(crate) fn swap_dirs(staged: &Path, live: &Path) -> std::io::Result<()> {
    match live.try_exists()? {
        false => std::fs::rename(staged, live),
        true => exchange(staged, live).or_else(|error| match error.kind() == std::io::ErrorKind::Unsupported {
            true => {
                tracing::warn!(%error, folder = %live.display(), "this filesystem cannot swap folders in one rename; moving the old folder aside first");
                move_aside(staged, live)
            }
            false => Err(error),
        }),
    }
}

/// Swap two existing directories with three renames, through `staged` with `-old` added; when the new folder cannot be put in place, the old one is put back. A stop between the first two renames leaves the old folder at the `-old` name, which the next load puts back (see `remove_staged`).
fn move_aside(staged: &Path, live: &Path) -> std::io::Result<()> {
    let mut aside = staged.as_os_str().to_owned();
    aside.push(MOVED_ASIDE_SUFFIX);
    let aside = PathBuf::from(aside);
    std::fs::rename(live, &aside)?;
    if let Err(error) = std::fs::rename(staged, live) {
        std::fs::rename(&aside, live)?;
        return Err(error);
    }
    std::fs::rename(&aside, staged)
}

/// Whether `entry` is a folder a load of the table named by `stem` staged in, `<stem><load id>`, and whether it is the old table moved aside, `<stem><load id>-old`.
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

/// Swap two existing directories in one rename; `Unsupported` where the system or filesystem has no such rename.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn exchange(staged: &Path, live: &Path) -> std::io::Result<()> {
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
fn exchange(_staged: &Path, _live: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// Copy into `staged` every file of `live` the load does not replace and that is not a pending marker: by hard link, or by copying the bytes where the filesystem has no hard links. Returns the key that failed and why.
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

/// The key below the table folder of a listed file the swap keeps: not replaced by the load, and not a pending marker.
fn kept_key(location: &ObjectPath, prefix: &str, replaced: &BTreeSet<String>) -> Option<String> {
    let key = location.as_ref().strip_prefix(prefix)?;
    (!replaced.contains(key) && !key.starts_with(PENDING_FOLDER)).then(|| key.to_owned())
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

/// Swap the staged folder in, then delete the old table now at `staged`; a missing staged folder (nothing written) is created first, and a failed delete only warns.
pub(crate) fn swap_in(staged: &Path, live: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(staged)?;
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

/// Delete the staging folders of table folder `live` (named by `stem`) in its parent; when `live` is missing because a stop came while its old table was moved aside, put that table back first.
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
    let mut missing = !live.try_exists()?;
    for (entry, aside) in entries {
        match aside && missing {
            true => {
                std::fs::rename(&entry, live)?;
                missing = false;
            }
            false => std::fs::remove_dir_all(&entry)?,
        }
    }
    Ok(())
}

/// A directory entry that is a staging folder of the table named by `stem`, and whether it is a moved-aside old table.
fn staged_dir(entry: &std::fs::DirEntry, stem: &str) -> Option<(PathBuf, bool)> {
    staged_entry(&entry.file_name().to_string_lossy(), stem).map(|aside| (entry.path(), aside))
}

/// Delete every hidden folder beside table folder `live` that a load of it staged in: one a stopped load left, or the old table a load swapped out but could not delete. When the table folder is missing because a load stopped while moving the old table aside, that old table is put back first. Best effort: a failure only warns, and the next load tries again.
pub(crate) async fn remove_staged(live: &Location) {
    let Some((path, _, name)) = local_folder(live) else {
        return;
    };
    let stem = staged_stem(name);
    let removed = tokio::task::spawn_blocking(move || clear_staged(&path, &stem)).await;
    if let Err(error) = removed
        .map_err(|error| error.to_string())
        .and_then(|removed| removed.map_err(|error| error.to_string()))
    {
        tracing::warn!(%error, folder = %live, "cannot delete a staging folder an earlier load left beside the table; the next load tries again");
    }
}
