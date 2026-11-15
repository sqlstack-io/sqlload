//! The deletes and moves of `after_load`, file by file.

use object_store::ObjectStoreExt;

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

/// The files `after_load` handles of the `settled` ones, whose rows committed, among those `read`: each file as it is, and each ZIP archive once for its members, when every member read from it committed and the archive is still the version read (its ETag, else its modified time, which its members carry); the archives left, and why. An archive whose members did not all commit stays, as another load reads them.
pub(crate) async fn archives(
    folder: &OpenFolder,
    read: &[StoredFile],
    settled: &[StoredFile],
) -> (Vec<StoredFile>, Vec<AfterLoadFailure>) {
    let mut files: Vec<StoredFile> = Vec::new();
    let mut failed: Vec<AfterLoadFailure> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for file in settled {
        let Some(key) = archive_key(file) else {
            files.push(file.clone());
            continue;
        };
        if seen.contains(&key) {
            continue;
        }
        seen.push(key.clone());
        if let Some(member) = unsettled_member((read, settled), &key) {
            failed.push(failure(
                folder,
                &key,
                format!(
                    "its member {} did not commit, so the archive stays where it is",
                    member.member.as_deref().unwrap_or_default()
                ),
            ));
            continue;
        }
        match folder.store.head(&file.path).await {
            Ok(meta)
                if file.etag.as_ref().map_or(
                    meta.last_modified.timestamp_millis() == file.modified_ms,
                    |etag| meta.e_tag.as_ref() == Some(etag),
                ) =>
            {
                files.push(StoredFile {
                    path: file.path.clone(),
                    relative_key: key,
                    size: meta.size,
                    etag: meta.e_tag,
                    modified_ms: meta.last_modified.timestamp_millis(),
                    member: None,
                });
            }
            Ok(_) => failed.push(failure(
                folder,
                &key,
                "it changed since it was read, so its new rows are not loaded; it is left where it is"
                    .to_owned(),
            )),
            Err(error) => failed.push(failure(folder, &key, format!("cannot check: {error}"))),
        }
    }
    (files, failed)
}

/// The key of the ZIP archive `file` is a member of; `None` for a file of its own.
fn archive_key(file: &StoredFile) -> Option<String> {
    let member = file.member.as_ref()?;
    Some(
        file.relative_key
            .strip_suffix(&format!("/{member}"))
            .unwrap_or(&file.relative_key)
            .to_owned(),
    )
}

/// A member of the archive at `key` that was `read` but is not among the `settled` files.
fn unsettled_member<'a>(
    (read, settled): (&'a [StoredFile], &[StoredFile]),
    key: &str,
) -> Option<&'a StoredFile> {
    let settled_keys: Vec<&str> = settled
        .iter()
        .map(|file| file.relative_key.as_str())
        .collect();
    read.iter().find(|member| {
        archive_key(member).as_deref() == Some(key)
            && !settled_keys.contains(&member.relative_key.as_str())
    })
}
