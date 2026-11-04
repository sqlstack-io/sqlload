//! Stream files into Arrow batches shaped for the destination.

use futures_util::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::footers::object_reader;
use crate::files_source::_helpers::shape::{output_schema, shape_batch};
use crate::files_source::errors::FilesError;
use crate::files_source::models::FilesRead;

/// Read every file of `request` in order, passing each shaped batch to `on_batch`; returns the row count.
pub async fn read_files(
    folder: &OpenFolder,
    request: &FilesRead,
    mut on_batch: impl AsyncFnMut(arrow_array::RecordBatch) -> Result<(), String>,
) -> Result<u64, FilesError> {
    let schema = output_schema(&request.columns).map_err(|reason| FilesError::File {
        key: folder.location.to_string(),
        reason,
    })?;
    let mut rows: u64 = 0;
    for data in &request.files {
        let failed = |reason: String| FilesError::File {
            key: data.file.relative_key.clone(),
            reason,
        };
        let file_name = format!("{}{}", folder.location, data.file.relative_key);
        let mut stream = ParquetRecordBatchStreamBuilder::new(object_reader(folder, &data.file))
            .await
            .and_then(|builder| builder.with_batch_size(request.batch_rows).build())
            .map_err(|error| failed(error.to_string()))?;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| failed(error.to_string()))?
        {
            let shaped = shape_batch(&batch, data, &file_name, &schema).map_err(failed)?;
            rows += shaped.num_rows() as u64;
            on_batch(shaped).await.map_err(failed)?;
        }
    }
    Ok(rows)
}
