//! List a files table's data files by partition.

use crate::column_types::models::ColumnType;
use crate::file_store::errors::StoreError;
use crate::file_store::main::list_files::list_files;
use crate::file_store::models::OpenFolder;
use crate::files_destination::_helpers::partitions::group_partitions;
use crate::files_destination::models::{TableFiles, TablePath};

/// The Parquet files under the table's `folder`, grouped by partition (oldest first) when `path` has partitions.
pub async fn list_table_files(
    folder: &OpenFolder,
    path: &TablePath,
    cursor_type: Option<&ColumnType>,
) -> Result<TableFiles, StoreError> {
    Ok(group_partitions(
        list_files(folder).await?,
        path,
        cursor_type,
    ))
}
