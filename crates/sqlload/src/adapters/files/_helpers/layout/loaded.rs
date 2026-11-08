use std::collections::BTreeSet;

use futures_util::{StreamExt, TryStreamExt, stream};

use crate::adapters::files::constants::{LOADED_FOLDER, LOADED_SUFFIX, RECORD_READS};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::LoadedFile;
use crate::store_paths::models::Location;

/// The key, below the table folder, of the record of the source files commit `file_id` loaded.
pub(crate) fn loaded_key(file_id: &str) -> String {
    format!("{LOADED_FOLDER}{file_id}{LOADED_SUFFIX}")
}

/// The folder of a table's records of the source files its loads loaded, in the same store; listed on its own, as listing the table skips its hidden `_sqlload/` folder.
pub(crate) fn loaded_folder(folder: &OpenFolder) -> OpenFolder {
    OpenFolder {
        store: folder.store.clone(),
        location: Location {
            store: folder.location.store.clone(),
            prefix: format!("{}{LOADED_FOLDER}", folder.location.prefix),
        },
    }
}

/// The commit whose record `name` (in the records folder) is, if it is one.
pub(crate) fn record_commit(name: &str) -> Option<&str> {
    name.strip_suffix(LOADED_SUFFIX)
        .filter(|commit| !commit.is_empty() && !commit.contains('/'))
}

/// A record's text: one `[file, version]` array per source file whose rows its commit's files hold.
pub(crate) fn loaded_text(files: &[LoadedFile]) -> Result<String, String> {
    let rows: Vec<[&str; 2]> = files
        .iter()
        .map(|file| [file.name.as_str(), file.version.as_str()])
        .collect();
    serde_json::to_string(&rows).map_err(|error| error.to_string())
}

/// The source files a record's text lists.
pub(crate) fn parse_loaded(text: &str) -> Result<Vec<LoadedFile>, String> {
    let rows: Vec<[String; 2]> = serde_json::from_str(text).map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|[name, version]| LoadedFile { name, version })
        .collect())
}

/// The source files recorded below `folder` by commits the table still holds (one of `commits`), and the keys (below `folder`) of records of commits it no longer holds, which repair deletes; the listing alone tells them apart, and only live records are read, a few at a time.
pub(crate) async fn loaded_records(
    folder: &OpenFolder,
    commits: &BTreeSet<String>,
) -> Result<(Vec<LoadedFile>, Vec<String>), FilesDestinationError> {
    let records = loaded_folder(folder);
    let mut live: Vec<String> = Vec::new();
    let mut stale: Vec<String> = Vec::new();
    for file in list_files(&records).await? {
        match record_commit(&file.relative_key).map(|commit| commits.contains(commit)) {
            Some(true) => live.push(file.relative_key),
            Some(false) => stale.push(format!("{LOADED_FOLDER}{}", file.relative_key)),
            None => {}
        }
    }
    let read = stream::iter(live.iter())
        .map(|key| read_record(&records, key))
        .buffer_unordered(RECORD_READS)
        .try_collect::<Vec<Vec<LoadedFile>>>()
        .await?;
    Ok((read.into_iter().flatten().collect(), stale))
}

/// The source files record `key` (in the records folder) lists.
async fn read_record(
    records: &OpenFolder,
    key: &str,
) -> Result<Vec<LoadedFile>, FilesDestinationError> {
    let text = read_file(records, key).await?;
    parse_loaded(&String::from_utf8_lossy(&text)).map_err(|reason| FilesDestinationError::Write {
        key: format!("{}{key}", records.location),
        reason,
    })
}
