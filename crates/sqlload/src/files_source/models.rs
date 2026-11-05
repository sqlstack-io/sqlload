use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use parquet::file::metadata::ParquetMetaData;
use serde::Deserialize;

use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::schema_plan::models::DesiredColumn;

/// A data file and its partition values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFile {
    pub file: StoredFile,
    /// Values of the source's declared partition columns, from the file's `key=value` folders.
    pub partitions: Vec<(String, String)>,
}

/// One column in the files, and the portable type it maps to (or why it has none).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileColumn {
    pub name: String,
    pub column_type: Result<ColumnType, String>,
    /// How the column is held when a direct `COPY` does not load it exactly, among the files this was read from.
    pub direct_refusal: Option<String>,
    /// How a stage read returns the column's values, where that takes more than a cast.
    pub stored_read: StoredRead,
}

/// How a stage read returns a Parquet column's values (with `USE_LOGICAL_TYPE`), where loading them exactly takes more than a cast.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StoredRead {
    #[default]
    Plain,
    /// Nanosecond times or timestamps: kept whole, unless the live column holds fewer digits.
    Nanos,
    /// A bare integer an embedded Arrow type names a timestamp or date: units of 10^-scale seconds since the epoch.
    Epoch(u8),
}

/// What a files source holds: its data files and their combined schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesTable {
    /// Every data file, sorted by key.
    pub files: Vec<DataFile>,
    /// Columns in the files, in first-seen order (newest file first).
    pub columns: Vec<FileColumn>,
    /// Problems found while combining footers (a column with different types in different files).
    pub problems: Vec<String>,
    /// Listed files skipped because their names do not fit the format and compression.
    pub skipped: usize,
    /// The loader's own columns found in the files (as they spell them), dropped for this load's.
    pub dropped: Vec<String>,
}

/// Which files to read and the columns to produce, in table order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesRead {
    pub files: Vec<DataFile>,
    pub encoding: FilesEncoding,
    /// What JSON keys and CSV headers may hold (unused for Parquet).
    pub checks: TextChecks,
    /// Data columns, partition columns and the source-file column, as the table has them.
    pub columns: Vec<DesiredColumn>,
    /// Time and timestamp columns read at nanoseconds, as the files hold them, where the destination keeps them.
    pub nanos: Vec<String>,
    pub batch_rows: usize,
}

/// One batch's Parquet footers by relative key, read once; a file missing here has its footer read when it is.
#[derive(Debug, Clone, Default)]
pub struct BatchFooters(pub std::collections::BTreeMap<String, ArrowReaderMetadata>);

/// The columns a JSON or CSV read checks its keys and values against.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextChecks {
    /// Every column of the source (sampled, pinned, partition and source-file columns, excluded ones too); any other key or header fails the read.
    pub known: Vec<String>,
    /// `exclude_columns` patterns: a key or header they match is dropped even when sampling never saw it.
    pub exclude: Vec<String>,
    /// `allow_columns` patterns, which win over `exclude`.
    pub allow: Vec<String>,
    /// Columns whose types were inferred from sampled values: every value must follow the same rules (pinned columns convert leniently instead).
    pub inferred: Vec<String>,
}

/// A source file already in the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedFile {
    pub name: String,
    /// The version it was loaded at; empty for rows loaded before versions were recorded.
    pub version: String,
}

/// What decides which files a load reads.
#[derive(Debug, Clone, Copy)]
pub struct FileSelection<'a> {
    pub files: &'a [DataFile],
    /// The folder as it prints, the prefix of every recorded source file name.
    pub location: &'a str,
    /// Source files already in the destination, with their versions (empty when unknown).
    pub loaded: &'a [LoadedFile],
    /// The partition column the cursor is on, and its type.
    pub cursor: Option<(&'a str, &'a ColumnType)>,
    pub from: Option<&'a str>,
    pub to: Option<&'a str>,
}

/// One Parquet file's footer: its Arrow schema and its metadata (row groups and statistics).
#[derive(Debug, Clone)]
pub struct Footer {
    pub relative_key: String,
    pub schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
}

/// The file formats a files source reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    #[default]
    Parquet,
    /// One JSON object per line.
    Ndjson,
    /// JSON objects one after another (over any number of lines), or one top-level array of objects.
    Json,
    Csv,
}

impl std::fmt::Display for SourceFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Parquet => "parquet",
            Self::Ndjson => "ndjson",
            Self::Json => "json",
            Self::Csv => "csv",
        })
    }
}

/// How the files of a text format are compressed; Parquet compresses inside the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// By file name: `.gz` is gzip, `.zst` is zstd, anything else is uncompressed.
    #[default]
    Auto,
    None,
    Gzip,
    Zstd,
}

/// The decompression one file is read with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Plain,
    Gzip,
    Zstd,
}

/// How CSV files are written.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CsvOptions {
    /// The field separator, one ASCII character.
    pub delimiter: char,
    /// Whether the first record holds the column names; without one, columns are `column_1`, `column_2`, ...
    pub header: bool,
    /// The quote character, one ASCII character; a quote inside a quoted field is written twice.
    pub quote: char,
    /// Field values read as NULL.
    pub null_values: Vec<String>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            header: true,
            quote: '"',
            null_values: vec![String::new()],
        }
    }
}

/// How a files source's files are encoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilesEncoding {
    pub format: SourceFormat,
    pub compression: Compression,
    pub csv: CsvOptions,
}

impl std::fmt::Display for FilesEncoding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.format, self.compression) {
            (SourceFormat::Parquet, _) => write!(formatter, "{}", self.format),
            (format, Compression::Auto) => write!(formatter, "{format}, compression by file name"),
            (format, Compression::None) => write!(formatter, "{format}, uncompressed"),
            (format, Compression::Gzip) => write!(formatter, "{format}, gzip"),
            (format, Compression::Zstd) => write!(formatter, "{format}, zstd"),
        }
    }
}

/// One file of a read, and what its batches are shaped into.
#[derive(Debug)]
pub(crate) struct FileRead<'a> {
    pub(crate) folder: &'a OpenFolder,
    pub(crate) request: &'a FilesRead,
    pub(crate) data: &'a DataFile,
    pub(crate) schema: &'a SchemaRef,
    /// The file as recorded in the source-file column.
    pub(crate) file_name: String,
}
