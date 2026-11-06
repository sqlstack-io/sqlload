//! Replacing the project file atomically, as sqlbuild's `write_atomically` does.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// A temporary name beside `path`, unique to this process and moment.
fn temporary_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    path.with_file_name(format!(".{name}.{}{nanos:x}", std::process::id()))
}

/// Write and sync `contents` to a new `temporary` beside `path`, with its permissions.
fn stage_replacement(path: &Path, temporary: &Path, contents: &str) -> std::io::Result<()> {
    let permissions = fs::metadata(path)?.permissions();
    let mut handle: File = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)?;
    handle.set_permissions(permissions)?;
    handle.write_all(contents.as_bytes())?;
    handle.sync_all()
}

/// Replace `path` (read as `original`) with `contents` atomically, keeping its permissions; refused when the file changed since it was read, so no edit made meanwhile is lost.
pub(crate) fn replace_file(path: &Path, (original, contents): (&str, &str)) -> Result<(), String> {
    let failure = |error: std::io::Error| format!("cannot write {}: {error}", path.display());
    if fs::read_to_string(path).map_err(failure)? != original {
        return Err(format!(
            "{} changed while contract generate ran; nothing was written, run it again",
            path.display()
        ));
    }
    let temporary = temporary_path(path);
    let written =
        stage_replacement(path, &temporary, contents).and_then(|()| fs::rename(&temporary, path));
    let _ = fs::remove_file(&temporary);
    written.map_err(failure)
}
