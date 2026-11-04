/// The column holding each row's source file, used to skip files already loaded by `append`.
pub const SOURCE_FILE_COLUMN: &str = "_sqlload_source_file";

/// The file name suffix of Parquet data files.
pub const PARQUET_SUFFIX: &str = ".parquet";

/// Footers read to work out the schema: the newest files by key, so recent schema changes are seen.
pub const SCHEMA_SAMPLE_FILES: usize = 20;
