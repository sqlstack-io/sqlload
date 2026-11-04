//! Read the footers of listed Parquet files.

use futures_util::{StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::parquet::footers::object_reader;
use crate::files_source::constants::CONCURRENT_FOOTER_READS;
use crate::files_source::errors::FilesError;
use crate::files_source::models::Footer;

/// The footer of each of `files` (range reads, several at once), in the order given.
pub async fn read_footers(
    folder: &OpenFolder,
    files: &[StoredFile],
) -> Result<Vec<Footer>, FilesError> {
    futures_util::stream::iter(files)
        .map(|file| read_footer(folder, file))
        .buffered(CONCURRENT_FOOTER_READS)
        .try_collect()
        .await
}

async fn read_footer(folder: &OpenFolder, file: &StoredFile) -> Result<Footer, FilesError> {
    let builder = ParquetRecordBatchStreamBuilder::new(object_reader(folder, file))
        .await
        .map_err(|error| FilesError::File {
            key: file.relative_key.clone(),
            reason: format!("cannot read the Parquet footer: {error}"),
        })?;
    Ok(Footer {
        relative_key: file.relative_key.clone(),
        schema: builder.schema().clone(),
        metadata: builder.metadata().clone(),
    })
}
