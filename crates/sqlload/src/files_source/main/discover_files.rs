//! List a files source and read its schema from Parquet footers.

use crate::file_store::main::list_files::list_files;
use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::columns::merge_schema;
use crate::files_source::_helpers::footers::footer_schema;
use crate::files_source::_helpers::partitions::data_file;
use crate::files_source::constants::{PARQUET_SUFFIX, SCHEMA_SAMPLE_FILES};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{DataFile, FileColumn, FilesTable};

/// The Parquet files under `folder` with their `partition_columns` values, and the schema of the newest ones.
pub async fn discover_files(
    folder: &OpenFolder,
    partition_columns: &[String],
) -> Result<FilesTable, FilesError> {
    let mut files: Vec<DataFile> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    for file in list_files(folder).await? {
        if !file
            .relative_key
            .to_ascii_lowercase()
            .ends_with(PARQUET_SUFFIX)
        {
            continue;
        }
        let (data, problem) = data_file(file, partition_columns);
        problems.extend(problem);
        files.push(data);
    }
    if files.is_empty() {
        return Err(FilesError::File {
            key: folder.location.to_string(),
            reason: "no .parquet files found; check the source's path (an empty source never empties the destination)".to_owned(),
        });
    }
    let mut columns: Vec<FileColumn> = Vec::new();
    for data in files.iter().rev().take(SCHEMA_SAMPLE_FILES) {
        let schema = footer_schema(folder, &data.file).await?;
        let (merged, conflicts) = merge_schema(columns, &schema, &data.file.relative_key);
        columns = merged;
        problems.extend(conflicts);
    }
    Ok(FilesTable {
        files,
        columns,
        problems,
    })
}
