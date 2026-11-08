use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::parquet::object_reader::ObjectFileReader;
use crate::files_source::errors::FilesError;

/// A reader for one listed Parquet file; its size is known, so no request is spent finding it.
pub(crate) fn object_reader(folder: &OpenFolder, file: &StoredFile) -> ObjectFileReader {
    ObjectFileReader {
        store: folder.store.clone(),
        path: file.path.clone(),
        size: file.size,
    }
}

/// `file`'s footer, with its Arrow and Parquet schemas (a range read, not the whole file).
pub(crate) async fn parquet_footer(
    folder: &OpenFolder,
    file: &StoredFile,
) -> Result<ArrowReaderMetadata, FilesError> {
    let mut reader = object_reader(folder, file);
    ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
        .await
        .map_err(|error| FilesError::File {
            key: file.relative_key.clone(),
            reason: format!("cannot read the Parquet footer: {error}"),
        })
}
