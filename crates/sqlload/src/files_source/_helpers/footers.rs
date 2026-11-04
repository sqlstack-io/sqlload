use arrow_schema::SchemaRef;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::object_reader::ObjectFileReader;
use crate::files_source::errors::FilesError;

/// A reader for one Parquet file, sized so no extra request is needed to find its footer.
pub(crate) fn object_reader(folder: &OpenFolder, file: &StoredFile) -> ObjectFileReader {
    ObjectFileReader {
        store: folder.store.clone(),
        path: Path::from(format!("{}{}", folder.location.prefix, file.relative_key)),
        size: file.size,
    }
}

/// The Arrow schema in `file`'s footer (a range read, not the whole file).
pub(crate) async fn footer_schema(
    folder: &OpenFolder,
    file: &StoredFile,
) -> Result<SchemaRef, FilesError> {
    ParquetRecordBatchStreamBuilder::new(object_reader(folder, file))
        .await
        .map(|builder| builder.schema().clone())
        .map_err(|error| FilesError::File {
            key: file.relative_key.clone(),
            reason: format!("cannot read the Parquet footer: {error}"),
        })
}
