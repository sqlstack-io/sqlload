use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::adapters::files::_helpers::layout::loads::load_id_of;
use crate::adapters::files::constants::{
    COMPACTED_PREFIX, LOADED_FOLDER, LOADED_SUFFIX, RECORD_READS, RECORDS_COMPACTED_ABOVE,
};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{LiveRecords, LoadedRecord};
use crate::adapters::files::types::CommitFiles;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::models::LoadedFile;
use crate::store_paths::models::Location;

/// What a table's records say: the commits whose files it holds, the source files they loaded, why an older SQLLoad's record cannot be read, its live records, and the keys of stale ones.
pub(crate) struct TableRecords {
    pub commits: BTreeSet<String>,
    pub loaded: Vec<LoadedFile>,
    pub old_records: Option<String>,
    pub records: LiveRecords,
    pub stale: Vec<String>,
}

/// The key, below the table folder, of the record `name` names (a commit's id, or a merged record's name).
pub(crate) fn loaded_key(name: &str) -> String {
    format!("{LOADED_FOLDER}{name}{LOADED_SUFFIX}")
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

/// The commit whose own record `name` (in the records folder) is, if it is one; a merged record is no commit's.
pub(crate) fn record_commit(name: &str) -> Option<&str> {
    name.strip_suffix(LOADED_SUFFIX).filter(|commit| {
        !commit.is_empty() && !commit.contains('/') && !commit.starts_with(COMPACTED_PREFIX)
    })
}

/// The folder of the file at `key` below the table folder, ending in `/`, or empty at the top.
pub(crate) fn folder_of(key: &str) -> &str {
    key.rfind('/').map_or("", |slash| &key[..=slash])
}

/// Put the record named `name` holding `commits`.
pub(crate) async fn put_record(
    folder: &OpenFolder,
    name: &str,
    commits: &CommitFiles,
) -> Result<(), FilesDestinationError> {
    let key = loaded_key(name);
    let record = LoadedRecord {
        commits: commits.clone(),
    };
    let text = serde_json::to_string(&record).map_err(|error| FilesDestinationError::Write {
        key: format!("{}{key}", folder.location),
        reason: error.to_string(),
    })?;
    Ok(write_file(folder, &key, Bytes::from(text)).await?)
}

/// While the load holds the table's lease, once it has more than [`RECORDS_COMPACTED_ABOVE`] live records, put one record holding what they all still hold, then delete them; a stop in between leaves both, which count the same files once. A record an older SQLLoad wrote is never live, so it is kept.
pub(crate) async fn compact_records(
    folder: &OpenFolder,
    records: &LiveRecords,
) -> Result<(), FilesDestinationError> {
    if records.keys.len() <= RECORDS_COMPACTED_ABOVE {
        return Ok(());
    }
    let name = format!("{COMPACTED_PREFIX}{}", uuid::Uuid::new_v4());
    put_record(folder, &name, &records.groups).await?;
    Ok(delete_files(folder, &records.keys).await?)
}

/// The records of `kept`, the table's trusted data files: by the folders each commit's files are in, a commit's own record is stale from its name alone, unread; merged records are always read, and stale once they hold nothing held.
pub(crate) async fn table_records(
    folder: &OpenFolder,
    kept: &[StoredFile],
) -> Result<TableRecords, FilesDestinationError> {
    let mut held: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for file in kept {
        if let Some(commit) = load_id_of(&file.relative_key) {
            let folders = held.entry(commit.to_owned()).or_default();
            folders.insert(folder_of(&file.relative_key).to_owned());
        }
    }
    let records = loaded_folder(folder);
    let mut read: Vec<String> = Vec::new();
    let mut stale: Vec<String> = Vec::new();
    for file in list_files(&records).await? {
        let key = file.relative_key;
        let merged = key.starts_with(COMPACTED_PREFIX) && key.ends_with(LOADED_SUFFIX);
        match (
            record_commit(&key).map(|commit| held.contains_key(commit)),
            merged,
        ) {
            (Some(true), _) | (None, true) => read.push(key),
            (Some(false), _) => stale.push(format!("{LOADED_FOLDER}{key}")),
            (None, false) => {}
        }
    }
    let texts: Vec<(String, Option<LoadedRecord>)> = stream::iter(read.iter())
        .map(|key| read_record(&records, key))
        .buffer_unordered(RECORD_READS)
        .try_collect()
        .await?;
    let mut live = LiveRecords::default();
    let mut old: Option<String> = None;
    for (key, parsed) in texts {
        let Some(record) = parsed else {
            old.get_or_insert(format!(
                "{}{key} is a record of loaded source files in the format of an older SQLLoad, which this one cannot read. Fix: load the table with --reload, which rewrites its files and records",
                records.location
            ));
            continue;
        };
        let kept = held_groups(record.commits, &held);
        let key = format!("{LOADED_FOLDER}{key}");
        match kept.is_empty() {
            true => stale.push(key),
            false => live.keys.push(key),
        }
        for (commit, folders) in kept {
            let group = live.groups.entry(commit).or_default();
            for (written, files) in folders {
                group.entry(written).or_default().extend(files);
            }
        }
    }
    live.keys.sort();
    Ok(TableRecords {
        commits: held.into_keys().collect(),
        loaded: loaded_files(&live.groups),
        old_records: old,
        records: live,
        stale,
    })
}

/// The groups of `commits` in a folder `held` says their commit still has files in.
fn held_groups(commits: CommitFiles, held: &BTreeMap<String, BTreeSet<String>>) -> CommitFiles {
    let mut kept = CommitFiles::new();
    for (commit, folders) in commits {
        let Some(held_folders) = held.get(&commit) else {
            continue;
        };
        let folders: BTreeMap<String, Vec<(String, String)>> = folders
            .into_iter()
            .filter(|(written, _)| held_folders.contains(written))
            .collect();
        if !folders.is_empty() {
            kept.insert(commit, folders);
        }
    }
    kept
}

/// Every source file `groups` hold, once each.
fn loaded_files(groups: &CommitFiles) -> Vec<LoadedFile> {
    let mut files: BTreeSet<&(String, String)> = BTreeSet::new();
    for folders in groups.values() {
        files.extend(folders.values().flatten());
    }
    files
        .into_iter()
        .map(|(name, version)| LoadedFile {
            name: name.clone(),
            version: version.clone(),
        })
        .collect()
}

/// What record `key` (in the records folder) holds; `None` for a record in the format SQLLoad wrote before records were kept per folder (a list of files), which cannot tell which folder holds a file's rows.
async fn read_record(
    records: &OpenFolder,
    key: &str,
) -> Result<(String, Option<LoadedRecord>), FilesDestinationError> {
    let text = read_file(records, key).await?;
    let failed = |error: serde_json::Error| FilesDestinationError::Write {
        key: format!("{}{key}", records.location),
        reason: error.to_string(),
    };
    let value: serde_json::Value = serde_json::from_slice(&text).map_err(failed)?;
    let record: Option<LoadedRecord> = match value.is_array() {
        true => None,
        false => Some(serde_json::from_value(value).map_err(failed)?),
    };
    Ok((key.to_owned(), record))
}
