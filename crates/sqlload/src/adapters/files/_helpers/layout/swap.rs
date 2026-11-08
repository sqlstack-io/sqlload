//! The hidden folder a filesystem `replace` stages a table in, beside its folder, and the rename that swaps the two.

use std::path::{Path, PathBuf};

use crate::adapters::files::constants::STAGED_FOLDER_MARK;
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

/// Swap two existing directories with three renames, through a name beside `staged`.
fn move_aside(staged: &Path, live: &Path) -> std::io::Result<()> {
    let mut aside = staged.as_os_str().to_owned();
    aside.push("-old");
    let aside = PathBuf::from(aside);
    std::fs::rename(live, &aside)?;
    std::fs::rename(staged, live)?;
    std::fs::rename(&aside, staged)
}

/// Swap two existing directories in one rename; `Unsupported` where the system or filesystem has no such rename.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn exchange(staged: &Path, live: &Path) -> std::io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    use rustix::io::Errno;
    renameat_with(CWD, staged, CWD, live, RenameFlags::EXCHANGE).map_err(|errno| {
        match [Errno::INVAL, Errno::NOSYS, Errno::OPNOTSUPP].contains(&errno) {
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
