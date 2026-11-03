/// The file format clause for gzip NDJSON files.
pub const JSON_FILE_FORMAT: &str = "FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO)";
/// The file format clause for Parquet files; logical types make timestamps and dates typed.
pub const PARQUET_FILE_FORMAT: &str = "FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)";
pub const JSON_EXTENSION: &str = "json.gz";
pub const PARQUET_EXTENSION: &str = "parquet";
