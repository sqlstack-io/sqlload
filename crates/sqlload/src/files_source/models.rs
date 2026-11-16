use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use parquet::file::metadata::ParquetMetaData;
use serde::Deserialize;

use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::DEFAULT_CSV_ENCODING;
use crate::row_batches::models::Flatten;
use crate::schema_plan::models::DesiredColumn;

/// A data file and its partition values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFile {
    pub file: StoredFile,
    /// Values of the source's declared partition columns: from the file's `key=value` folders, captured by `path_columns`, and `constant_columns`.
    pub partitions: Vec<(String, String)>,
}

/// The columns a files source fills from each file's path or its configuration rather than from the files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathValues {
    /// `partition_columns`: Hive `key=value` folders.
    pub partition_columns: Vec<String>,
    /// `path_columns`: values captured from the key below the source's path.
    pub template: Option<PathTemplate>,
    /// What a file whose key does not match `template` does.
    pub unmatched: UnmatchedPaths,
    /// `constant_columns`: the same value, as text, for every file.
    pub constants: Vec<(String, String)>,
}

impl PathValues {
    /// Every column these values fill, in table order: Hive folders, captures, then constants.
    pub fn names(&self) -> Vec<&str> {
        self.partition_columns
            .iter()
            .map(String::as_str)
            .chain(
                self.template
                    .iter()
                    .flat_map(|template| template.captures.iter())
                    .map(|(name, _)| name.as_str()),
            )
            .chain(self.constants.iter().map(|(name, _)| name.as_str()))
            .collect()
    }
}

/// A `path_columns` template, compiled: one regex group per capture, in order.
#[derive(Debug, Clone)]
pub struct PathTemplate {
    /// The template as written.
    pub text: String,
    pub pattern: regex::Regex,
    /// Each capture's column and type, in the order of the regex's groups.
    pub captures: Vec<(String, ColumnType)>,
}

/// Templates are equal when written alike, as their patterns are compiled from the text.
impl PartialEq for PathTemplate {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text && self.captures == other.captures
    }
}

impl Eq for PathTemplate {}

/// What a file whose key does not match `path_columns` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnmatchedPaths {
    /// `plan` fails, naming the file.
    #[default]
    Fail,
    /// The file is not loaded; `plan` counts it.
    Skip,
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
    /// Whether the sampled values were lists, for `unnest = "all"`; `None` while every one was NULL or empty.
    pub lists: Option<bool>,
    /// The header this column repeats, when a file's header has it more than once; its `name` is then numbered (`Qty_1`).
    pub repeat_of: Option<String>,
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
    /// Listed files skipped because their keys do not match `path_columns` (with `unmatched_paths = "skip"`).
    pub unmatched: usize,
    /// The loader's own columns found in the files (as they spell them), dropped for this load's.
    pub dropped: Vec<String>,
    /// The objects `flatten` flattened into `<object>__<key>` columns in the sampled JSON records, by those names, sorted (a key holding `__` inside an object is not one).
    pub objects: Vec<String>,
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

/// One batch's Parquet footers by store key (`StoredFile::store_key`), read once; a file missing here has its footer read when it is.
#[derive(Debug, Clone, Default)]
pub struct BatchFooters {
    pub parquet: std::collections::BTreeMap<String, ArrowReaderMetadata>,
    /// CSV files' column names by store key, read once where the destination reads the files itself (by position).
    pub headers: std::collections::BTreeMap<String, Vec<String>>,
}

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
    /// Whether loaded files leave the folder once they load (`after_load`), so a loaded name at another version is a new file, not one rewritten in place.
    pub removed: bool,
    /// Source files a `delete_insert` on a partition cursor holds, with their versions: a listed one at another version whose cursor value lies below `from` fails, rather than being skipped and keeping its old rows. Empty otherwise.
    pub held: &'a [LoadedFile],
    /// The partition column the cursor is on, and its type.
    pub cursor: Option<(&'a str, &'a ColumnType)>,
    /// How a file's key gives its partition values, which the `held` files' names give too.
    pub paths: &'a PathValues,
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
    /// Excel workbooks: one worksheet's rows, each cell typed as the workbook stores it.
    Xlsx,
    /// XML documents: each element at `xml.record_path` one row.
    Xml,
}

impl std::fmt::Display for SourceFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Parquet => "parquet",
            Self::Ndjson => "ndjson",
            Self::Json => "json",
            Self::Csv => "csv",
            Self::Xlsx => "xlsx",
            Self::Xml => "xml",
        })
    }
}

/// What a files source does with each file once its rows commit: keep it (the default), delete it, or move it to another folder of the connection, keeping its key below `path`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AfterLoad {
    #[default]
    Keep,
    Delete,
    MoveTo(String),
}

/// The order a files source's merge applies its files in, and commits them one each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesOrder {
    /// Oldest modified first, files modified at the same time in path order.
    #[default]
    Modified,
    /// By full path below the source's `path` (a ZIP member's as `<archive>.zip/<member>`), in code point order, as text cursors compare.
    Path,
}

/// What a load does when a files source's folder exists and holds no files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnEmpty {
    /// Fail, as an empty folder can mean a broken mount (`after_load` still skips a folder it emptied).
    #[default]
    Fail,
    /// Skip the table, leaving it as it is.
    Skip,
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
    /// ZIP archives (`.zip`): each member whose name fits the format is a file of its own.
    Zip,
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
    /// The quote character, one ASCII character, whose quoted fields may hold the delimiter and line breaks (a quote inside one written twice, or after `escape`); `false` reads every quote as a plain character.
    pub quote: CsvQuote,
    /// Inside a quoted field, the character that makes the next one plain text, such as `\\` for `\"`; none by default.
    pub escape: Option<char>,
    /// Whether a file read as `latin-1`, `cp1252` or `iso-8859-15` fails when its bytes say it is in another encoding: control characters that mean cp1252, or mostly UTF-8 sequences.
    pub encoding_check: bool,
    /// Field values read as NULL.
    pub null_values: Vec<String>,
    /// The files' character encoding, such as `utf-8` (the default), `cp1252` or `latin-1`; see [`text_charset`](crate::files_source::main::text_charset::text_charset).
    pub encoding: String,
    /// Lines skipped at the start of each file, before the header (or the first record without one).
    pub skip_rows: u64,
    /// The columns' names without a header, from the source's or table's `column_names`.
    #[serde(skip)]
    pub column_names: Option<Vec<String>>,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            header: true,
            quote: CsvQuote::Char('"'),
            escape: None,
            encoding_check: true,
            null_values: vec![String::new()],
            encoding: DEFAULT_CSV_ENCODING.to_owned(),
            skip_rows: 0,
            column_names: None,
        }
    }
}

/// A CSV source's `quote` setting: the quote character, or `false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum CsvQuote {
    Char(char),
    /// `false` turns quoting off; `true` is refused at plan, as it names no character.
    Off(bool),
}

impl CsvQuote {
    /// The quote character; `None` when quoting is off.
    pub fn character(self) -> Option<char> {
        match self {
            Self::Char(character) => Some(character),
            Self::Off(_) => None,
        }
    }
}

/// How CSV bytes decode to text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    /// UTF-8, read as it is.
    Utf8,
    /// ISO-8859-1: each byte is the character of the same number.
    Latin1,
    /// US-ASCII: a byte above 127 is an error.
    Ascii,
    /// Any other encoding the WHATWG Encoding Standard names, such as windows-1252 or UTF-16LE.
    Other(&'static encoding_rs::Encoding),
}

impl Charset {
    /// Whether `csv.encoding_check` checks files read in this charset (`latin-1`, `cp1252` and `iso-8859-15`), and if so whether C1 control characters count against it (`latin-1` and `iso-8859-15`, where no text holds them); `None` for any other.
    pub fn checked(self) -> Option<bool> {
        match self {
            Self::Latin1 => Some(true),
            Self::Other(encoding) if encoding == encoding_rs::ISO_8859_15 => Some(true),
            Self::Other(encoding) if encoding == encoding_rs::WINDOWS_1252 => Some(false),
            Self::Utf8 | Self::Ascii | Self::Other(_) => None,
        }
    }
}

/// Which rows of an Excel workbook load.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct XlsxOptions {
    /// The worksheet, by name; the first one when omitted.
    pub sheet: Option<String>,
    /// Whether the first row read holds the column names; without one, columns are `column_1`, `column_2`, ... by their place in the range (the sheet without one), or `column_names`.
    pub header: bool,
    /// Rows skipped at the top of the sheet, before the header (or the first row without one).
    pub skip_rows: u32,
    /// An A1 range such as `Prices!A3:C`: the sheet, columns and rows read, in place of `sheet` and `skip_rows`.
    pub range: Option<String>,
    /// The header's row, counting from 1 at the first row read; the rows above it are skipped. The first row holding a value when omitted.
    pub header_row: Option<u32>,
    /// The columns' names without a header row, from the source's or table's `column_names`.
    #[serde(skip)]
    pub column_names: Option<Vec<String>>,
}

impl Default for XlsxOptions {
    fn default() -> Self {
        Self {
            sheet: None,
            header: true,
            skip_rows: 0,
            range: None,
            header_row: None,
            column_names: None,
        }
    }
}

/// Which elements of an XML document are rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct XmlOptions {
    /// The element names from the root to each row's element, separated by `/`, such as `orders/order`.
    pub record_path: String,
    /// Child elements loaded as a JSON array even where one appears only once.
    pub lists: Vec<String>,
}

/// How a files source's files are encoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilesEncoding {
    pub format: SourceFormat,
    pub compression: Compression,
    pub csv: CsvOptions,
    pub xlsx: XlsxOptions,
    pub xml: XmlOptions,
    /// With `compression = "zip"`, a pattern the members' names must match, beyond the format's suffix.
    pub members: Option<String>,
    /// The glob each file's path below `path` must match, in place of the format's name suffix.
    pub pattern: Option<FilePattern>,
    /// With `shape = "record"`, the one column each JSON object or CSV line loads into whole; `None` for typed columns.
    pub record: Option<String>,
    /// Which of a JSON record's fields holding objects load as one column per key, `<field>__<key>` (none when a `transform` runs, whose output flattens instead).
    pub flatten: Flatten,
}

/// A files source's `pattern`: a glob over each file's path below `path`, `*` within one name and `**` across folders.
#[derive(Debug, Clone)]
pub struct FilePattern {
    pub text: String,
    matcher: globset::GlobMatcher,
}

impl PartialEq for FilePattern {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl Eq for FilePattern {}

impl FilePattern {
    /// `text` as a pattern, or why it isn't a glob.
    pub fn new(text: &str) -> Result<Self, globset::Error> {
        let glob = globset::GlobBuilder::new(text)
            .literal_separator(true)
            .build()?;
        Ok(Self {
            text: text.to_owned(),
            matcher: glob.compile_matcher(),
        })
    }

    /// Whether the file at `key`, its path below the source's `path`, matches.
    pub fn is_match(&self, key: &str) -> bool {
        self.matcher.is_match(key)
    }
}

impl FilesEncoding {
    /// How the plan names the files a source chose: `listed`, or `matching <pattern>`.
    pub fn chosen_text(&self) -> String {
        match &self.pattern {
            Some(pattern) => format!("matching {}", pattern.text),
            None => "listed".to_owned(),
        }
    }

    /// How the plan names the `count` files listing left out: those that are no data file of this format, or match no `pattern`.
    pub fn skipped_text(&self, count: usize) -> String {
        match (count, &self.pattern) {
            (0, _) => String::new(),
            (1, Some(_)) => ", 1 other file ignored".to_owned(),
            (count, Some(_)) => format!(", {count} other files ignored"),
            (count, None) => format!(", {count} skipped (names that do not fit {self})"),
        }
    }
}

/// How a JSON or CSV source's records load: as typed columns sampled from them, or each whole in one `json` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordShape {
    #[default]
    Columns,
    Record,
}

impl std::fmt::Display for FilesEncoding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.format, self.compression) {
            (SourceFormat::Parquet, _) => write!(formatter, "{}", self.format),
            (format, Compression::Auto) => write!(formatter, "{format}, compression by file name"),
            (format, Compression::None) => write!(formatter, "{format}, uncompressed"),
            (format, Compression::Gzip) => write!(formatter, "{format}, gzip"),
            (format, Compression::Zstd) => write!(formatter, "{format}, zstd"),
            (format, Compression::Zip) => write!(formatter, "{format} in ZIP archives"),
        }?;
        match &self.record {
            Some(column) => write!(formatter, ", each record whole in column {column}"),
            None => Ok(()),
        }
    }
}

/// The files a source reads, and what listing left out or found wrong.
#[derive(Debug)]
pub(crate) struct ListedFiles {
    pub(crate) files: Vec<(DataFile, Codec)>,
    /// Files whose names do not fit the format and compression.
    pub(crate) skipped: usize,
    /// Files whose keys do not match `path_columns`, skipped.
    pub(crate) unmatched: usize,
    pub(crate) problems: Vec<String>,
}

/// One value of a spreadsheet row or XML record: its text, which the shape step converts to the column's type, and the type it suggests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cell {
    pub(crate) text: String,
    pub(crate) kind: ColumnType,
}

/// One spreadsheet row or XML record: its cells by column name, in order, a NULL cell left out; and the row or line it starts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CellRecord {
    pub(crate) line: u64,
    pub(crate) cells: Vec<(String, Cell)>,
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

/// What `after_load` did with a table's loaded files: `status` is `done`, or `failed` when any file stayed where it was (the load still counts, and is not run again).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct AfterLoadReport {
    /// `delete` or `move`.
    pub action: &'static str,
    pub status: &'static str,
    /// Files deleted or moved.
    pub files: u64,
    pub failed: Vec<AfterLoadFailure>,
}

/// A loaded file `after_load` could not delete or move, and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct AfterLoadFailure {
    pub file: String,
    pub error: String,
}
