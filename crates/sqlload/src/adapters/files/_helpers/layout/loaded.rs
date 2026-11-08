use std::collections::BTreeSet;

use crate::adapters::files::constants::{LOADED_FOLDER, LOADED_SUFFIX};
use crate::adapters::files::errors::FilesDestinationError;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::LoadedFile;
use crate::store_paths::models::Location;

/// The key, below the table folder, of the record of the source files load `load_id` loaded.
pub(crate) fn loaded_key(load_id: &str) -> String {
    format!("{LOADED_FOLDER}{load_id}{LOADED_SUFFIX}")
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

/// Whether `name` (in the records folder) is a load's record of the source files it loaded.
pub(crate) fn is_loaded_record(name: &str) -> bool {
    name.ends_with(LOADED_SUFFIX)
}

/// A record's text: one `[commit, file, version]` array per source file, the commit being the one whose files hold its rows.
pub(crate) fn loaded_text(entries: &[(String, LoadedFile)]) -> Result<String, String> {
    let rows: Vec<[&str; 3]> = entries
        .iter()
        .map(|(commit, file)| [commit.as_str(), file.name.as_str(), file.version.as_str()])
        .collect();
    serde_json::to_string(&rows).map_err(|error| error.to_string())
}

/// The entries of a record's text, each with its commit.
pub(crate) fn parse_loaded(text: &str) -> Result<Vec<(String, LoadedFile)>, String> {
    let rows: Vec<[String; 3]> = serde_json::from_str(text).map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|[commit, name, version]| (commit, LoadedFile { name, version }))
        .collect())
}

/// The source files recorded below `folder` whose commit the table still holds (one of `commits`), and the records left holding none (keys below `folder`), which repair deletes.
pub(crate) async fn loaded_records(
    folder: &OpenFolder,
    commits: &BTreeSet<String>,
) -> Result<(Vec<LoadedFile>, Vec<String>), FilesDestinationError> {
    let mut loaded: Vec<LoadedFile> = Vec::new();
    let mut stale: Vec<String> = Vec::new();
    let records = loaded_folder(folder);
    for record in list_files(&records)
        .await?
        .into_iter()
        .filter(|file| is_loaded_record(&file.relative_key))
    {
        let text = read_file(&records, &record.relative_key).await?;
        let entries = parse_loaded(&String::from_utf8_lossy(&text)).map_err(|reason| {
            FilesDestinationError::Write {
                key: format!("{}{}", records.location, record.relative_key),
                reason,
            }
        })?;
        let held: Vec<LoadedFile> = entries
            .into_iter()
            .filter(|(commit, _)| commits.contains(commit))
            .map(|(_, file)| file)
            .collect();
        if held.is_empty() {
            stale.push(format!("{LOADED_FOLDER}{}", record.relative_key));
        }
        loaded.extend(held);
    }
    Ok((loaded, stale))
}
