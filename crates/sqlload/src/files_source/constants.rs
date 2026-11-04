/// The column holding each row's source file, used to skip files already loaded by `append`.
pub const SOURCE_FILE_COLUMN: &str = "_sqlload_source_file";

/// The column holding the version of each row's source file (its ETag, or size and modification time), to notice files rewritten in place.
pub const SOURCE_VERSION_COLUMN: &str = "_sqlload_source_version";

/// Separates a file name from its version in the loaded-files query; versions never contain it.
pub const VERSION_SEPARATOR: char = '|';

/// The file name suffix of Parquet data files.
pub const PARQUET_SUFFIX: &str = ".parquet";

/// Footers read to work out the schema: the newest files by key, so recent schema changes are seen.
pub const SCHEMA_SAMPLE_FILES: usize = 20;
