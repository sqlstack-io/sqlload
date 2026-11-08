use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::adapters::files::_helpers::write::csv_files::{
    csv_bytes, csv_header, csv_rows, finish_csv, new_csv, push_csv,
};
use crate::adapters::files::constants::{
    CSV_SUFFIX, GZIP_SUFFIX, OPEN_FILE_OVERHEAD_BYTES, PARQUET_SUFFIX, PART_PREFIX,
    SINGLE_FILE_STEM, ZSTD_SUFFIX,
};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::models::{FileBody, FilesWriter, OpenFile};
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::project::models::{FilesFormat, OutputCompression, OutputEncoding};

/// An empty file in memory with the table's schema: zstd Parquet whose statistics are never truncated, so maximums are exact; or CSV starting with its header line when it has one.
pub(crate) fn new_file(schema: &SchemaRef, encoding: &OutputEncoding) -> Result<OpenFile, String> {
    let body = match encoding.format {
        FilesFormat::Parquet => {
            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_statistics_truncate_length(None)
                .set_column_index_truncate_length(None)
                .build();
            FileBody::Parquet(
                ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties))
                    .map_err(|error| error.to_string())?,
            )
        }
        FilesFormat::Csv => {
            let out = new_csv(encoding.compression);
            FileBody::Csv(match encoding.csv.header {
                true => push_csv(out, &csv_header(schema, &encoding.csv))?,
                false => out,
            })
        }
    };
    Ok(OpenFile { body, rows: 0 })
}

/// `file` with `batch`'s rows added.
pub(crate) fn add_rows(
    mut file: OpenFile,
    batch: &RecordBatch,
    encoding: &OutputEncoding,
) -> Result<OpenFile, String> {
    file.body = match file.body {
        FileBody::Parquet(mut writer) => {
            writer.write(batch).map_err(|error| error.to_string())?;
            FileBody::Parquet(writer)
        }
        FileBody::Csv(out) => FileBody::Csv(push_csv(out, &csv_rows(batch, &encoding.csv)?)?),
    };
    file.rows += batch.num_rows();
    Ok(file)
}

/// The bytes an open file holds in memory: written, still buffered, and the writer's own overhead.
pub(crate) fn open_bytes(file: &OpenFile) -> usize {
    let held = match &file.body {
        FileBody::Parquet(writer) => {
            writer.bytes_written() + writer.in_progress_size().max(writer.memory_size())
        }
        FileBody::Csv(out) => csv_bytes(out),
    };
    held + OPEN_FILE_OVERHEAD_BYTES
}

/// The key of the writer's next file in partition `value`: `<partition folder>/part-<load id>-<n><suffix>`, or with `single_file` the table folder's own name, `<partition folder>/<table><suffix>`.
pub(crate) fn next_key(writer: &FilesWriter, value: &str) -> String {
    let folder = match &writer.partition {
        Some(partition) => format!("{}/", partition.name(value)),
        None => String::new(),
    };
    let suffix = file_suffix(&writer.encoding);
    match writer.encoding.single_file {
        true => {
            let stem = writer
                .folder
                .location
                .prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .filter(|name| !name.is_empty())
                .unwrap_or(SINGLE_FILE_STEM);
            format!("{folder}{stem}{suffix}")
        }
        false => format!(
            "{folder}{PART_PREFIX}{}-{}{suffix}",
            writer.file_id,
            writer.written.len()
        ),
    }
}

/// The name suffix of the files `encoding` writes: `.parquet`, or `.csv` with `.gz` or `.zst` when compressed.
pub(crate) fn file_suffix(encoding: &OutputEncoding) -> String {
    match (encoding.format, encoding.compression) {
        (FilesFormat::Parquet, _) => PARQUET_SUFFIX.to_owned(),
        (FilesFormat::Csv, OutputCompression::None) => CSV_SUFFIX.to_owned(),
        (FilesFormat::Csv, OutputCompression::Gzip) => format!("{CSV_SUFFIX}{GZIP_SUFFIX}"),
        (FilesFormat::Csv, OutputCompression::Zstd) => format!("{CSV_SUFFIX}{ZSTD_SUFFIX}"),
    }
}

/// Finish `file` and put it at `key` in one write, so readers see it only once complete.
pub(crate) async fn store_file(
    folder: &OpenFolder,
    key: &str,
    file: OpenFile,
) -> Result<(), FilesDestinationError> {
    let failed = |reason: String| FilesDestinationError::Write {
        key: key.to_owned(),
        reason,
    };
    let bytes = finish_file(file).map_err(failed)?;
    Ok(write_file(folder, key, Bytes::from(bytes)).await?)
}

/// The complete bytes of `file`: the Parquet footer written, or the CSV compression finished.
pub(crate) fn finish_file(file: OpenFile) -> Result<Vec<u8>, String> {
    match file.body {
        FileBody::Parquet(writer) => writer.into_inner().map_err(|error| error.to_string()),
        FileBody::Csv(out) => finish_csv(out),
    }
}
