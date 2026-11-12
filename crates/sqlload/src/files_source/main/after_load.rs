//! Delete or move a files source's files once their rows are committed.

use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::move_file::move_file;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::{AfterLoad, AfterLoadFailure, AfterLoadReport};

/// Do `after_load` to the files `relative_keys` under `folder`, whose rows are committed: delete them all at once, or move each to its key below `move_to`, never overwriting; a file that stays is reported with why, and warned about, but never fails the load. `None` when the files are kept.
pub async fn after_load(
    folder: &OpenFolder,
    after_load: &AfterLoad,
    relative_keys: &[String],
) -> Option<AfterLoadReport> {
    let (action, failed) = match after_load {
        AfterLoad::Keep => return None,
        AfterLoad::Delete => ("delete", deleted(folder, relative_keys).await),
        AfterLoad::MoveTo(move_to) => ("move", moved(folder, move_to, relative_keys).await),
    };
    for failure in &failed {
        tracing::warn!(file = %failure.file, error = %failure.error, "after_load {action} failed; the file stays where it was, and its rows stay loaded");
    }
    let files = relative_keys.len().saturating_sub(failed.len());
    Some(AfterLoadReport {
        action,
        status: match failed.is_empty() {
            true => "done",
            false => "failed",
        },
        files: u64::try_from(files).unwrap_or(u64::MAX),
        failed,
    })
}

/// The files of a bulk delete that failed: all of them, as the store does not say which.
async fn deleted(folder: &OpenFolder, relative_keys: &[String]) -> Vec<AfterLoadFailure> {
    match relative_keys.is_empty() {
        true => Vec::new(),
        false => match delete_files(folder, relative_keys).await {
            Ok(()) => Vec::new(),
            Err(error) => relative_keys
                .iter()
                .map(|key| AfterLoadFailure {
                    file: format!("{}{key}", folder.location),
                    error: error.to_string(),
                })
                .collect(),
        },
    }
}

/// The files that could not be moved below `move_to`.
async fn moved(
    folder: &OpenFolder,
    move_to: &str,
    relative_keys: &[String],
) -> Vec<AfterLoadFailure> {
    let mut failed: Vec<AfterLoadFailure> = Vec::new();
    for key in relative_keys {
        let to = format!("{}/{key}", move_to.trim_matches('/'));
        if let Err(error) = move_file(folder, key, &to).await {
            failed.push(AfterLoadFailure {
                file: format!("{}{key}", folder.location),
                error: error.to_string(),
            });
        }
    }
    failed
}
