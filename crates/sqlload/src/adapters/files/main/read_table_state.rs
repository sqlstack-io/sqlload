//! List a files table: its data files by partition, loads that did not finish, and the source files its loads loaded.

use std::collections::BTreeSet;

use crate::adapters::files::_helpers::layout::loaded::{is_loaded_record, parse_loaded};
use crate::adapters::files::_helpers::layout::loads::load_id_of;
use crate::adapters::files::_helpers::layout::markers::{parse_marker, pending_folder};
use crate::adapters::files::_helpers::layout::partitions::group_partitions;
use crate::adapters::files::constants::PARQUET_SUFFIX;
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{PendingLoad, PendingPhase, TablePath, TableState};
use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::models::LoadedFile;

/// The table's files grouped by partition (oldest first), without the files repairing an unfinished load deletes, and those loads.
pub async fn read_table_state(
    folder: &OpenFolder,
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
) -> Result<TableState, FilesDestinationError> {
    let files = list_files(folder).await?;
    let markers = pending_folder(folder);
    let mut pending: Vec<PendingLoad> = Vec::new();
    for marker in list_files(&markers).await? {
        let text = read_file(&markers, &marker.relative_key).await?;
        let marker_read = parse_marker(&String::from_utf8_lossy(&text)).map_err(|reason| {
            FilesDestinationError::Write {
                key: format!("{}{}", markers.location, marker.relative_key),
                reason,
            }
        })?;
        let deletes = match marker_read.phase {
            PendingPhase::Writing => files_of(&files, &marker.relative_key),
            PendingPhase::Replacing => marker_read.keys,
        };
        pending.push(PendingLoad {
            load_id: marker.relative_key,
            phase: marker_read.phase,
            deletes,
            run: marker_read.run,
        });
    }
    let dropped: BTreeSet<&str> = pending
        .iter()
        .flat_map(|load| load.deletes.iter())
        .map(String::as_str)
        .collect();
    let exists = files.iter().any(|file| {
        file.relative_key
            .to_ascii_lowercase()
            .ends_with(PARQUET_SUFFIX)
    });
    let kept: Vec<StoredFile> = files
        .iter()
        .filter(|file| !dropped.contains(file.relative_key.as_str()))
        .cloned()
        .collect();
    let commits: BTreeSet<String> = kept
        .iter()
        .filter_map(|file| load_id_of(&file.relative_key).map(str::to_owned))
        .collect();
    let mut loaded: Vec<LoadedFile> = Vec::new();
    let mut stale_loaded: Vec<String> = Vec::new();
    for record in files
        .iter()
        .filter(|file| is_loaded_record(&file.relative_key))
    {
        let text = read_file(folder, &record.relative_key).await?;
        let entries = parse_loaded(&String::from_utf8_lossy(&text)).map_err(|reason| {
            FilesDestinationError::Write {
                key: format!("{}{}", folder.location, record.relative_key),
                reason,
            }
        })?;
        let held: Vec<LoadedFile> = entries
            .into_iter()
            .filter(|(commit, _)| commits.contains(commit))
            .map(|(_, file)| file)
            .collect();
        if held.is_empty() {
            stale_loaded.push(record.relative_key.clone());
        }
        loaded.extend(held);
    }
    Ok(TableState {
        listed: group_partitions(kept, path, cursor_type),
        pending,
        exists,
        loaded,
        stale_loaded,
    })
}

/// The keys of `load_id`'s files anywhere below the table folder.
fn files_of(files: &[StoredFile], load_id: &str) -> Vec<String> {
    files
        .iter()
        .filter(|file| load_id_of(&file.relative_key) == Some(load_id))
        .map(|file| file.relative_key.clone())
        .collect()
}
