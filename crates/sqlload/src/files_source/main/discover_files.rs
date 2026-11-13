//! List a files source and work out its schema from Parquet footers or sampled JSON and CSV records.

use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::archive::zip_archive::archive_members;
use crate::files_source::_helpers::plan::columns::{
    ColumnMerge, SampledColumn, case_problems, drop_loader_columns, file_columns, merge_columns,
};
use crate::files_source::_helpers::plan::encoding::{file_codec, file_names};
use crate::files_source::_helpers::plan::folder::unreadable;
use crate::files_source::_helpers::plan::partitions::data_file;
use crate::files_source::_helpers::plan::sample::newest_columns;
use crate::files_source::constants::{
    SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN, SOURCE_VERSION_COLUMN,
};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{
    Compression, FilesEncoding, FilesTable, ListedFiles, PathValues, SourceFormat, StoredRead,
};

/// The `encoding` files under `folder` with their path `values`, and the schema of the newest ones without the loader's own columns, which this load fills; when a file's path lacks a value, only the listing and its problems, before any file is read.
pub async fn discover_files(
    folder: &OpenFolder,
    values: &PathValues,
    encoding: &FilesEncoding,
) -> Result<FilesTable, FilesError> {
    let listed = listed_files(folder, values, encoding).await?;
    let (files, mut problems) = (listed.files, listed.problems);
    if !problems.is_empty() {
        return Ok(FilesTable {
            files: files.into_iter().map(|(data, _)| data).collect(),
            columns: Vec::new(),
            problems,
            skipped: listed.skipped,
            unmatched: listed.unmatched,
            dropped: Vec::new(),
        });
    }
    let replaced: Vec<&str> = values
        .names()
        .into_iter()
        .chain([SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN, SOURCE_ROW_COLUMN])
        .collect();
    if let Some(record) = &encoding.record {
        return Ok(FilesTable {
            files: files.into_iter().map(|(data, _)| data).collect(),
            columns: file_columns(vec![record_column(record)]),
            problems,
            skipped: listed.skipped,
            unmatched: listed.unmatched,
            dropped: Vec::new(),
        });
    }
    let text_format = encoding.format != SourceFormat::Parquet;
    let mut columns: Vec<SampledColumn> = Vec::new();
    let mut dropped: Vec<String> = Vec::new();
    let sampled = newest_columns(folder, &files, encoding).await?;
    for ((data, _), found) in files.iter().rev().zip(sampled) {
        let (found, loader) = drop_loader_columns(found);
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
        skipped: listed.skipped,
        unmatched: listed.unmatched,
        dropped,
    })
}

/// The one `json` column a `shape = "record"` source loads each record into.
fn record_column(name: &str) -> SampledColumn {
    SampledColumn {
        name: name.to_owned(),
        column_type: Some(Ok(ColumnType::Json)),
        direct_refusal: None,
        stored_read: StoredRead::Plain,
        lists: Some(false),
    }
}

/// The listed files `encoding` reads and `values` keeps, sorted by key, with their decompression; how many others were skipped; and a problem for each file whose path lacks a value.
async fn listed_files(
    folder: &OpenFolder,
    values: &PathValues,
    encoding: &FilesEncoding,
) -> Result<ListedFiles, FilesError> {
    let mut listed = ListedFiles {
        files: Vec::new(),
        skipped: 0,
        unmatched: 0,
        problems: Vec::new(),
    };
    for file in list_files(folder).await? {
        let Some(codec) = file_codec(&file.relative_key, encoding) else {
            listed.skipped += 1;
            continue;
        };
        let files = match encoding.compression {
            Compression::Zip => {
                archive_members(folder, &file, encoding)
                    .await
                    .map_err(|reason| FilesError::File {
                        key: file.relative_key.clone(),
                        reason,
                    })?
            }
            _ => vec![file],
        };
        for file in files {
            let (data, problem) = data_file(file, values);
            listed.problems.extend(problem);
            match data {
                Some(data) => listed.files.push((data, codec)),
                None => listed.unmatched += 1,
            }
        }
    }
    let unmatched = match listed.unmatched {
        0 => String::new(),
        count => format!("; {count} listed files do not match path_columns"),
    };
    let reason = format!(
        "no {} files found{unmatched}; check the source's path and format (an empty source never empties the destination)",
        file_names(encoding)
    );
    let key = folder.location.to_string();
    let others = listed.skipped + listed.unmatched;
    match (
        listed.files.is_empty(),
        others,
        unreadable(&folder.location),
    ) {
        (false, _, _) => Ok(listed),
        (true, 0, None) => Err(FilesError::NoFiles { key, reason }),
        (true, _, None) => Err(FilesError::File { key, reason }),
        (true, _, Some(error)) => Err(FilesError::File {
            key,
            reason: format!("{reason}; the folder cannot be read: {error}"),
        }),
    }
}
