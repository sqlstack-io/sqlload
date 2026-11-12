//! The deletes and moves of `after_load`, file by file.

use crate::file_store::main::current_version::current_version;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::move_file::move_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::main::move_to_prefix::move_to_prefix;
use crate::files_source::models::AfterLoadFailure;

/// The files left by a delete: those changed since they were read, and every unchanged one when the bulk delete fails, as the store does not say which.
pub(crate) async fn deleted(folder: &OpenFolder, files: &[StoredFile]) -> Vec<AfterLoadFailure> {
    let mut failed: Vec<AfterLoadFailure> = Vec::new();
    let mut unchanged: Vec<String> = Vec::new();
    for file in files {
        match current_version(folder, file).await {
            Ok(()) => unchanged.push(file.relative_key.clone()),
            Err(error) => failed.push(failure(folder, &file.relative_key, error.to_string())),
        }
    }
    if unchanged.is_empty() {
        return failed;
    }
    if let Err(error) = delete_files(folder, &unchanged).await {
        failed.extend(
            unchanged
                .iter()
                .map(|key| failure(folder, key, error.to_string())),
        );
    }
    failed
}

/// The files that could not be moved below `move_to` (see [`move_to_prefix`]).
pub(crate) async fn moved(
    folder: &OpenFolder,
    move_to: &str,
    files: &[StoredFile],
) -> Vec<AfterLoadFailure> {
    let prefix = match move_to_prefix(&folder.location, move_to) {
        Ok(prefix) => prefix,
        Err(error) => {
            return files
                .iter()
                .map(|file| failure(folder, &file.relative_key, error.clone()))
                .collect();
        }
    };
    let mut failed: Vec<AfterLoadFailure> = Vec::new();
    for file in files {
        let to = format!("{prefix}{}", file.relative_key);
        if let Err(error) = move_file(folder, file, &to).await {
            failed.push(failure(folder, &file.relative_key, error.to_string()));
        }
    }
    failed
}

/// `key` under `folder`, left for `error`.
pub(crate) fn failure(folder: &OpenFolder, key: &str, error: String) -> AfterLoadFailure {
    AfterLoadFailure {
        file: format!("{}{key}", folder.location),
        error,
    }
}
