//! Stream files into Arrow batches shaped for the destination.

use arrow_array::RecordBatch;
use futures_util::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::parquet::footers::object_reader;
use crate::files_source::_helpers::plan::encoding::file_codec;
use crate::files_source::_helpers::plan::shape::{output_schema, shape_batch};
use crate::files_source::_helpers::text::text_reader::TextReader;
use crate::files_source::errors::FilesError;
use crate::files_source::models::{BatchFooters, Codec, FileRead, FilesRead, SourceFormat};

/// Read every file of `request` in order, reusing `footers` where it has a file's, passing each shaped batch to `on_batch`; returns the row count.
pub async fn read_files(
    folder: &OpenFolder,
    request: &FilesRead,
    footers: &BatchFooters,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, FilesError> {
    let schema =
        output_schema(&request.columns, &request.nanos).map_err(|reason| FilesError::File {
            key: folder.location.to_string(),
            reason,
        })?;
    let mut rows: u64 = 0;
    for data in &request.files {
        let failed = |reason: String| FilesError::File {
            key: data.file.relative_key.clone(),
            reason,
        };
        let file = FileRead {
            folder,
            request,
            data,
            schema: &schema,
            file_name: format!("{}{}", folder.location, data.file.relative_key),
        };
        rows += match request.encoding.format {
            SourceFormat::Parquet => read_parquet(&file, footers, &mut on_batch).await,
            _ => read_text(&file, &mut on_batch).await,
        }
        .map_err(failed)?;
    }
    Ok(rows)
}

async fn read_parquet(
    file: &FileRead<'_>,
    footers: &BatchFooters,
    on_batch: &mut impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, String> {
    let reader = object_reader(file.folder, &file.data.file);
    let builder = match footers.0.get(&file.data.file.relative_key) {
        Some(footer) => ParquetRecordBatchStreamBuilder::new_with_metadata(reader, footer.clone()),
        None => ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|error| error.to_string())?,
    };
    let mut stream = builder
        .with_batch_size(file.request.batch_rows)
        .build()
        .map_err(|error| error.to_string())?;
    let mut rows: u64 = 0;
    while let Some(batch) = stream.try_next().await.map_err(|error| error.to_string())? {
        let shaped = shape_batch(&batch, file.data, (&file.file_name, rows), file.schema)
            .map_err(|error| error.reason)?;
        rows += shaped.num_rows() as u64;
        on_batch(shaped).await?;
    }
    Ok(rows)
}

/// Read a JSON or CSV file; a value that does not convert names its line.
async fn read_text(
    file: &FileRead<'_>,
    on_batch: &mut impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, String> {
    let codec =
        file_codec(&file.data.file.relative_key, &file.request.encoding).unwrap_or(Codec::Plain);
    let mut reader = TextReader::open(file.folder, &file.data.file, codec, file.request).await?;
    let mut rows: u64 = 0;
    while let Some((batch, lines)) = reader.next_batch().await? {
        let shaped = shape_batch(&batch, file.data, (&file.file_name, rows), file.schema).map_err(
            |error| match error.row.and_then(|row| lines.get(row)) {
                Some(line) => format!(
                    "line {line}: {}; fix the file, or pin a type that holds the value",
                    error.reason
                ),
                None => error.reason,
            },
        )?;
        rows += shaped.num_rows() as u64;
        on_batch(shaped).await?;
    }
    Ok(rows)
}
