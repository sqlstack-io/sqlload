use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_destination::constants::{PARQUET_SUFFIX, PART_PREFIX};
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::models::{FilesWriter, OpenFile};

/// An empty zstd Parquet file in memory with the table's schema.
pub(crate) fn new_file(schema: &SchemaRef) -> Result<OpenFile, String> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties))
        .map(|writer| OpenFile { writer, rows: 0 })
        .map_err(|error| error.to_string())
}

/// The bytes an open file holds in memory, written or still buffered.
pub(crate) fn open_bytes(file: &OpenFile) -> usize {
    file.writer.bytes_written()
        + file
            .writer
            .in_progress_size()
            .max(file.writer.memory_size())
}

/// The key of the writer's next file in partition `value`: `<partition folder>/part-<load id>-<n>.parquet`.
pub(crate) fn next_key(writer: &FilesWriter, value: &str) -> String {
    let folder = match &writer.partition {
        Some(partition) => format!("{}/", partition.name(value)),
        None => String::new(),
    };
    format!(
        "{folder}{PART_PREFIX}{}-{}{PARQUET_SUFFIX}",
        writer.load_id,
        writer.written.len()
    )
}

/// Finish `file` and put it at `key` in one write, so readers see it only once complete.
pub(crate) async fn store_file(
    folder: &OpenFolder,
    key: &str,
    file: OpenFile,
) -> Result<(), FilesDestinationError> {
    let failed = |error: parquet::errors::ParquetError| FilesDestinationError::Write {
        key: key.to_owned(),
        reason: error.to_string(),
    };
    let bytes = file.writer.into_inner().map_err(failed)?;
    Ok(write_file(folder, key, Bytes::from(bytes)).await?)
}
