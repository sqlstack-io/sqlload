//! List a files table: its data files by partition, and loads that did not finish.

use std::collections::BTreeSet;

use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_destination::_helpers::loads::load_id_of;
use crate::files_destination::_helpers::markers::{parse_marker, pending_folder};
use crate::files_destination::_helpers::partitions::group_partitions;
use crate::files_destination::constants::PARQUET_SUFFIX;
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::models::{PendingLoad, PendingPhase, TablePath, TableState};

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
        let (phase, replaced) =
            parse_marker(&String::from_utf8_lossy(&text)).map_err(|reason| {
                FilesDestinationError::Write {
                    key: format!("{}{}", markers.location, marker.relative_key),
                    reason,
                }
            })?;
        let deletes = match phase {
            PendingPhase::Writing => files_of(&files, &marker.relative_key),
            PendingPhase::Replacing => replaced,
        };
        pending.push(PendingLoad {
            load_id: marker.relative_key,
            phase,
            deletes,
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
    Ok(TableState {
        listed: group_partitions(kept, path, cursor_type),
        pending,
        exists,
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
