//! Delete or move a files source's files once their rows are committed.

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::settle::after_load::{deleted, moved};
use crate::files_source::models::{AfterLoad, AfterLoadReport};

/// Do `after_load` to `files` under `folder`, whose rows are committed: delete them all at once, or move each to its key below `move_to` (see `move_to_prefix`), never overwriting; a file whose version changed since it was read is left, since its new rows are not loaded. A file that stays is reported with why, and warned about, but never fails the load. `None` when the files are kept.
pub async fn after_load(
    folder: &OpenFolder,
    after_load: &AfterLoad,
    files: &[StoredFile],
) -> Option<AfterLoadReport> {
    let (action, failed) = match after_load {
        AfterLoad::Keep => return None,
        AfterLoad::Delete => ("delete", deleted(folder, files).await),
        AfterLoad::MoveTo(move_to) => ("move", moved(folder, move_to, files).await),
    };
    for failure in &failed {
        tracing::warn!(file = %failure.file, error = %failure.error, "after_load {action} failed; the file stays where it was, and its rows stay loaded");
    }
    let done = files.len().saturating_sub(failed.len());
    Some(AfterLoadReport {
        action,
        status: match failed.is_empty() {
            true => "done",
            false => "failed",
        },
        files: u64::try_from(done).unwrap_or(u64::MAX),
        failed,
    })
}
