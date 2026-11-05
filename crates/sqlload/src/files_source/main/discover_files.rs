//! List a files source and work out its schema from Parquet footers or sampled JSON and CSV records.

use crate::file_store::main::list_files::list_files;
use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::parquet::footers::footer_schema;
use crate::files_source::_helpers::plan::columns::{
    ColumnMerge, SampledColumn, case_problems, drop_loader_columns, file_columns, merge_columns,
    schema_columns,
};
use crate::files_source::_helpers::plan::encoding::{file_codec, file_names};
use crate::files_source::_helpers::plan::partitions::data_file;
use crate::files_source::_helpers::text::sample::sample_file;
use crate::files_source::constants::{
    SCHEMA_SAMPLE_FILES, SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{Codec, DataFile, FilesEncoding, FilesTable, SourceFormat};

/// The `encoding` files under `folder` with their `partition_columns` values, and the schema of the newest ones without the loader's own columns, which this load fills.
pub async fn discover_files(
    folder: &OpenFolder,
    partition_columns: &[String],
    encoding: &FilesEncoding,
) -> Result<FilesTable, FilesError> {
    let (files, skipped, mut problems) = listed_files(folder, partition_columns, encoding).await?;
    let replaced: Vec<&str> = partition_columns
        .iter()
        .map(String::as_str)
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
        .collect();
    let text_format = encoding.format != SourceFormat::Parquet;
    let mut columns: Vec<SampledColumn> = Vec::new();
    let mut dropped: Vec<String> = Vec::new();
    for (data, codec) in files.iter().rev().take(SCHEMA_SAMPLE_FILES) {
        let (found, loader) =
            drop_loader_columns(file_columns_of(folder, data, *codec, encoding).await?);
        for name in loader {
            if !dropped.contains(&name) {
                dropped.push(name);
            }
        }
        let merge = ColumnMerge {
            key: &data.file.relative_key,
            replaced: &replaced,
            widen_numbers: text_format,
        };
        let (merged, conflicts) = merge_columns(columns, found, merge);
        columns = merged;
        problems.extend(conflicts);
    }
    if text_format {
        problems.extend(case_problems(&columns));
    }
    Ok(FilesTable {
        files: files.into_iter().map(|(data, _)| data).collect(),
        columns: file_columns(columns),
        problems,
        skipped,
        dropped,
    })
}

/// The listed files `encoding` reads, sorted by key, with their decompression; how many others were skipped; and a problem for each missing partition folder.
async fn listed_files(
    folder: &OpenFolder,
    partition_columns: &[String],
    encoding: &FilesEncoding,
) -> Result<(Vec<(DataFile, Codec)>, usize, Vec<String>), FilesError> {
    let mut files: Vec<(DataFile, Codec)> = Vec::new();
    let mut skipped: usize = 0;
    let mut problems: Vec<String> = Vec::new();
    for file in list_files(folder).await? {
        let Some(codec) = file_codec(&file.relative_key, encoding) else {
            skipped += 1;
            continue;
        };
        let (data, problem) = data_file(file, partition_columns);
        problems.extend(problem);
        files.push((data, codec));
    }
    match files.is_empty() {
        true => Err(FilesError::File {
            key: folder.location.to_string(),
            reason: format!(
                "no {} files found; check the source's path and format (an empty source never empties the destination)",
                file_names(encoding)
            ),
        }),
        false => Ok((files, skipped, problems)),
    }
}

/// One file's columns: from its Parquet footer, or from its first JSON or CSV records.
async fn file_columns_of(
    folder: &OpenFolder,
    data: &DataFile,
    codec: Codec,
    encoding: &FilesEncoding,
) -> Result<Vec<SampledColumn>, FilesError> {
    match encoding.format {
        SourceFormat::Parquet => Ok(schema_columns(&footer_schema(folder, &data.file).await?)),
        _ => sample_file(folder, &data.file, codec, encoding)
            .await
            .map_err(|reason| FilesError::File {
                key: data.file.relative_key.clone(),
                reason,
            }),
    }
}
