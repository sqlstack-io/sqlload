//! List a files table: its data files by partition, loads that did not finish, and the source files its loads loaded.

use std::collections::BTreeSet;

use crate::adapters::files::_helpers::layout::loaded::table_records;
use crate::adapters::files::_helpers::layout::markers::pending_loads;
use crate::adapters::files::_helpers::layout::partitions::{group_partitions, is_data_file};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{TablePath, TableState};
use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::models::{OpenFolder, StoredFile};

/// The table's files grouped by partition (oldest first), without the files repairing an unfinished load deletes, and those loads; an `aborted` marker counts as one only while a file of its load is left, or once it expired.
pub async fn read_table_state(
    folder: &OpenFolder,
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
) -> Result<TableState, FilesDestinationError> {
    let files = list_files(folder).await?;
    let pending = pending_loads(folder, &files).await?;
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
    let table = table_records(folder, &kept).await?;
    Ok(TableState {
        listed: group_partitions(kept, path, cursor_type),
        pending,
        exists,
        commits: table.commits,
        loaded: table.loaded,
        old_records: table.old_records,
        records: table.records,
        stale_loaded: table.stale,
    })
}
