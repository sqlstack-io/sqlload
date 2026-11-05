//! Read the footers of one batch's Parquet files.

use std::collections::BTreeMap;

use futures_util::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::parquet::footers::object_reader;
use crate::files_source::constants::CONCURRENT_FOOTER_READS;
use crate::files_source::errors::FilesError;
use crate::files_source::models::{BatchFooters, DataFile};

/// Each of `files`' footers, by relative key, read once and a few at a time; reading the files reuses them, so nothing reads a footer twice.
pub async fn batch_footers(
    folder: &OpenFolder,
    files: &[DataFile],
) -> Result<BatchFooters, FilesError> {
    let read: Vec<(String, ArrowReaderMetadata)> = futures_util::stream::iter(files)
        .map(|data| read_footer(folder, data))
        .buffered(CONCURRENT_FOOTER_READS)
        .try_collect()
        .await?;
    Ok(BatchFooters(read.into_iter().collect::<BTreeMap<_, _>>()))
}

async fn read_footer(
    folder: &OpenFolder,
    data: &DataFile,
) -> Result<(String, ArrowReaderMetadata), FilesError> {
    let key = &data.file.relative_key;
    let mut reader = object_reader(folder, &data.file);
    ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
        .await
        .map(|footer| (key.clone(), footer))
        .map_err(|error| FilesError::File {
            key: key.clone(),
            reason: format!("cannot read the Parquet footer: {error}"),
        })
}
