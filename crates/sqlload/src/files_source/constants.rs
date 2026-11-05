use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};

/// The column holding each row's source file, used to skip files already loaded by `append`.
pub const SOURCE_FILE_COLUMN: &str = "_sqlload_source_file";

/// The column holding the version of each row's source file (its ETag, or size and modification time), to notice files rewritten in place.
pub const SOURCE_VERSION_COLUMN: &str = "_sqlload_source_version";

/// The column holding a row's number in its source file, from 1.
pub const SOURCE_ROW_COLUMN: &str = "_sqlload_source_row";

/// Separates a file name from its version in the loaded-files query; versions never contain it.
pub const VERSION_SEPARATOR: char = '|';

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

/// Footers read at once when a destination reads many.
pub const CONCURRENT_FOOTER_READS: usize = 16;

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
