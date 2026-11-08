//! Read back what a files target wrote: Parquet or CSV data files, and their keys.

use arrow_array::RecordBatch;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::store_paths::models::Location;

/// How each data file a files target writes is read back, by the end of its name.
type FileReader = fn(bytes::Bytes) -> Result<Vec<RecordBatch>, String>;
const FILE_READERS: &[(&str, FileReader)] = &[
    (".parquet", parquet_batches),
    (".csv.gz", gzip_csv_batches),
    (".csv.zst", zstd_csv_batches),
    (".csv", plain_csv_batches),
];
/// The start of the name of a data file a load writes.
const PART_PREFIX: &str = "part-";
/// Where the suffix of a CSV data file starts.
const CSV_SUFFIX: &str = ".csv";
/// The suffix of a Parquet data file.
const PARQUET_SUFFIX: &str = ".parquet";

/// The rows of a data file a files target wrote: Parquet, or CSV (gzip or zstd by its suffix) with a header, read with every column as text.
pub(crate) fn file_batches(key: &str, bytes: bytes::Bytes) -> Result<Vec<RecordBatch>, String> {
    let (_, read) = FILE_READERS
        .iter()
        .find(|(suffix, _)| key.ends_with(suffix))
        .ok_or_else(|| format!("{key}: not a data file"))?;
    read(bytes)
}

/// The rows of a Parquet file.
fn parquet_batches(bytes: bytes::Bytes) -> Result<Vec<RecordBatch>, String> {
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .and_then(|builder| builder.build())
        .map_err(|error| error.to_string())?
        .map(|batch| batch.map_err(|error| error.to_string()))
        .collect()
}

/// The rows of a gzip CSV file.
fn gzip_csv_batches(bytes: bytes::Bytes) -> Result<Vec<RecordBatch>, String> {
    let mut text = Vec::new();
    std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&bytes[..]), &mut text)
        .map_err(|error| error.to_string())?;
    csv_batches(text)
}

/// The rows of a zstd CSV file.
fn zstd_csv_batches(bytes: bytes::Bytes) -> Result<Vec<RecordBatch>, String> {
    csv_batches(zstd::decode_all(&bytes[..]).map_err(|error| error.to_string())?)
}

/// The rows of a plain CSV file.
fn plain_csv_batches(bytes: bytes::Bytes) -> Result<Vec<RecordBatch>, String> {
    csv_batches(bytes.to_vec())
}

/// The rows of CSV text with a header line, every column read as text.
fn csv_batches(text: Vec<u8>) -> Result<Vec<RecordBatch>, String> {
    let header = text.split(|byte| *byte == b'\n').next().unwrap_or_default();
    let fields: Vec<arrow_schema::Field> = String::from_utf8_lossy(header)
        .split(',')
        .map(|name| arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true))
        .collect();
    arrow_csv::ReaderBuilder::new(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
        .with_header(true)
        .build(std::io::Cursor::new(text))
        .map_err(|error| error.to_string())?
        .map(|batch| batch.map_err(|error| error.to_string()))
        .collect()
}

/// The data files in the table folder, sorted, each `part-<load>-<n>` file name shown as `part-*` before its suffix.
pub(crate) async fn data_keys(table: &Location) -> Result<Vec<String>, String> {
    let folder = open_folder(table).map_err(|error| error.to_string())?;
    let mut keys: Vec<String> = list_files(&folder)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|file| shown_key(&file.relative_key))
        .collect();
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// `key` with a `part-<load>-<n>` file name shown as `part-*` before its suffix.
fn shown_key(key: &str) -> String {
    let (folder, name) = key
        .rsplit_once('/')
        .map_or(("", key), |(folder, name)| (folder, name));
    let name = name
        .find(CSV_SUFFIX)
        .or_else(|| name.find(PARQUET_SUFFIX))
        .filter(|_| name.starts_with(PART_PREFIX))
        .map_or_else(
            || name.to_owned(),
            |at| format!("{PART_PREFIX}*{}", &name[at..]),
        );
    [folder, name.as_str()]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<&str>>()
        .join("/")
}
