use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};

/// The column holding each row's source file, used to skip files already loaded by `append`.
pub const SOURCE_FILE_COLUMN: &str = "_sqlload_source_file";

/// The column holding the version of each row's source file (its ETag, or size and modification time), to notice files rewritten in place.
pub const SOURCE_VERSION_COLUMN: &str = "_sqlload_source_version";

/// The column holding a row's number in its source file, from 1.
pub const SOURCE_ROW_COLUMN: &str = "_sqlload_source_row";

/// Separates a file name from its version in the loaded-files query; versions never contain it.
pub const VERSION_SEPARATOR: char = '|';

/// The column a `shape = "record"` source loads each record into, unless `record_column` names another.
pub const RECORD_COLUMN: &str = "record";

/// The file name suffix of Parquet data files.
pub const PARQUET_SUFFIX: &str = ".parquet";

/// The file name suffixes of JSON data files, for both `ndjson` and `json`.
pub const JSON_SUFFIXES: &[&str] = &[".ndjson", ".jsonl", ".json"];

/// The file name suffix of CSV data files.
pub const CSV_SUFFIX: &str = ".csv";

/// The file name suffix of gzip-compressed files.
pub const GZIP_SUFFIX: &str = ".gz";

/// The file name suffix of zstd-compressed files.
pub const ZSTD_SUFFIX: &str = ".zst";

/// Records read from each sampled JSON or CSV file to work out its column types.
pub const SAMPLE_ROWS: usize = 10_000;

/// The longest JSON object or CSV record read, so a broken file (an unclosed quote or brace) fails instead of filling memory.
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// Bytes read from a JSON or CSV file (after decompression) at a time.
pub const READ_CHUNK_BYTES: usize = 256 * 1024;

/// Files sampled to work out the schema (their footers, or their first records): the newest by key, so recent schema changes are seen.
pub const SCHEMA_SAMPLE_FILES: usize = 20;

/// Bytes read from the end of a Parquet file for its footer: one request holds a typical footer, and stays within a new connection's first TCP window (a larger read costs round trips on a distant store); a larger footer takes a second request.
pub const FOOTER_PREFETCH_BYTES: usize = 8 * 1024;

/// Parquet footers (or CSV header lines) read at once, at plan and for each batch.
pub const CONCURRENT_FOOTER_READS: usize = 16;

/// JSON and CSV files sampled at once at plan: each sample holds up to `SAMPLE_ROWS` records, so fewer than footers.
pub const CONCURRENT_TEXT_SAMPLES: usize = 4;

/// Columns the loader writes itself (load metadata and source tracking): a files source drops them from its files, matched ignoring case, and this load fills its own.
pub const LOADER_COLUMNS: &[&str] = &[
    LOAD_ID_COLUMN,
    LOADED_AT_COLUMN,
    SOURCE_FILE_COLUMN,
    SOURCE_VERSION_COLUMN,
    SOURCE_ROW_COLUMN,
];

/// The largest decimal precision a direct `COPY` loads exactly.
pub const MAX_DIRECT_DECIMAL_PRECISION: u8 = 38;

/// The decimal digits of a second a nanosecond time or timestamp holds; a live column with fewer gets values cut to microseconds.
pub const NANOSECOND_SCALE: u8 = 9;

/// A `path_columns` segment matching any number of folders.
pub const ANY_FOLDERS: &str = "**";

/// The type of a `path_columns` capture that names none.
pub const DEFAULT_CAPTURE_TYPE: &str = "text";

/// How a captured or constant date is stored.
pub const NORMAL_DATE: &str = "%Y-%m-%d";

/// How a captured or constant time is stored.
pub const NORMAL_TIME: &str = "%H:%M:%S%.6f";

/// How a captured or constant timestamp without a zone is stored.
pub const NORMAL_TIMESTAMP: &str = "%Y-%m-%d %H:%M:%S%.6f";

/// How a captured or constant timestamp with a zone is stored: in UTC, the offset right after the time, which every destination reads.
pub const NORMAL_TIMESTAMP_TZ: &str = "%Y-%m-%dT%H:%M:%S%.6f%:z";

/// Rows of a nested Parquet column written as JSON text at a time.
pub const NESTED_JSON_SLICE_ROWS: usize = 1024;

/// The most text one Arrow text array holds: its offsets are 32-bit.
pub const MAX_TEXT_ARRAY_BYTES: usize = i32::MAX as usize;

/// The encoding CSV files are read in unless `csv.encoding` says otherwise.
pub const DEFAULT_CSV_ENCODING: &str = "utf-8";

/// `csv.encoding` labels read as ISO-8859-1 itself, byte for byte; the Encoding Standard reads these as windows-1252, which differs in bytes 0x80 to 0x9F.
pub const LATIN1_LABELS: [&str; 5] = ["latin-1", "latin1", "iso-8859-1", "iso8859-1", "l1"];

/// `csv.encoding` labels read as US-ASCII itself, failing on any byte above 127; the Encoding Standard reads these as windows-1252.
pub const ASCII_LABELS: [&str; 3] = ["ascii", "us-ascii", "ansi_x3.4-1968"];

/// The windows-1252 bytes no character is assigned to, which Python's cp1252 refuses to decode.
pub const CP1252_UNDEFINED: [u8; 5] = [0x81, 0x8D, 0x8F, 0x90, 0x9D];

/// The fix an error about bytes that do not decode ends with.
pub const ENCODING_HINT: &str =
    "set csv.encoding to the files' encoding, such as \"cp1252\", \"latin-1\" or \"utf-16le\"";
