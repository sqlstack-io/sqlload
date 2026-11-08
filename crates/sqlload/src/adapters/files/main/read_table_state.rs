//! List a files table: its data files by partition, and loads that did not finish.

use std::collections::BTreeSet;

use crate::adapters::files::_helpers::layout::loads::load_id_of;
use crate::adapters::files::_helpers::layout::markers::{parse_marker, pending_folder};
use crate::adapters::files::_helpers::layout::partitions::{group_partitions, is_data_file};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{PendingLoad, PendingPhase, TablePath, TableState};
use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::models::{OpenFolder, StoredFile};

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
    let exists = files.iter().any(|file| is_data_file(&file.relative_key));
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
