//! List a files source and read its schema from Parquet footers.

use crate::file_store::main::list_files::list_files;
use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::columns::merge_schema;
use crate::files_source::_helpers::footers::footer_schema;
use crate::files_source::constants::{PARQUET_SUFFIX, SCHEMA_SAMPLE_FILES};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{DataFile, FileColumn, FilesTable};
use crate::store_paths::main::hive_values::hive_values;

/// The Parquet files under `folder` with their `partition_columns` values, and the schema of the newest ones.
pub async fn discover_files(
    folder: &OpenFolder,
    partition_columns: &[String],
) -> Result<FilesTable, FilesError> {
    let mut files: Vec<DataFile> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    for file in list_files(folder).await? {
        if !file.relative_key.ends_with(PARQUET_SUFFIX) {
            continue;
        }
        let mut partitions: Vec<(String, String)> = hive_values(&file.relative_key);
        partitions.retain(|(key, _)| partition_columns.contains(key));
        let found: Vec<&String> = partitions.iter().map(|(key, _)| key).collect();
        if let Some(missing) = partition_columns
            .iter()
            .find(|column| !found.contains(column))
        {
            problems.push(format!(
                "{} has no {missing}=... folder, but {missing} is a partition column",
                file.relative_key
            ));
        }
        files.push(DataFile { file, partitions });
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
