//! Read the column names of one batch's CSV files.

use std::collections::BTreeMap;

use futures_util::{StreamExt, TryStreamExt};

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::plan::encoding::file_codec;
use crate::files_source::_helpers::text::bytes_reader::open_bytes;
use crate::files_source::_helpers::text::csv_reader::CsvReader;
use crate::files_source::constants::CONCURRENT_FOOTER_READS;
use crate::files_source::errors::FilesError;
use crate::files_source::models::{DataFile, FilesEncoding};

/// Each of `files`' CSV column names (the header's, or `column_1`, ... without one), by relative key, read from the start of each file a few at a time; empty for a file with no records.
pub async fn batch_headers(
    folder: &OpenFolder,
    files: &[DataFile],
    encoding: &FilesEncoding,
) -> Result<BTreeMap<String, Vec<String>>, FilesError> {
    let read: Vec<(String, Vec<String>)> = futures_util::stream::iter(files)
        .map(|data| read_header(folder, data, encoding))
        .buffered(CONCURRENT_FOOTER_READS)
        .try_collect()
        .await?;
    Ok(read.into_iter().collect())
}

async fn read_header(
    folder: &OpenFolder,
    data: &DataFile,
    encoding: &FilesEncoding,
) -> Result<(String, Vec<String>), FilesError> {
    let key = &data.file.relative_key;
    let failed = |reason: String| FilesError::File {
        key: key.clone(),
        reason,
    };
    let codec = file_codec(key, encoding)
        .ok_or_else(|| failed("cannot tell how the file is compressed".to_owned()))?;
    let bytes = open_bytes(folder, &data.file, codec)
        .await
        .map_err(failed)?;
    let reader = CsvReader::open(bytes, &encoding.csv, 1)
        .await
        .map_err(failed)?;
    Ok((key.clone(), reader.names().to_vec()))
}
