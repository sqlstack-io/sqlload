/// The stage SQLLoad creates in each loader schema (stored upper-cased, `_SQLLOAD_STAGE`).
pub const MANAGED_STAGE_NAME: &str = "_sqlload_stage";

/// The folder under the stage URL holding each load's staged Parquet, one folder per load id.
pub const STAGED_FOLDER: &str = "_sqlload/";

/// The most files one `COPY` names, Snowflake's limit for a `FILES` list.
pub const MAX_FILES_PER_COPY: usize = 1000;

/// The longest `COPY` statement built; a batch of files whose statement would be longer is split.
pub const MAX_COPY_SQL_BYTES: usize = 512 * 1024;

/// How Snowflake reads the Parquet files.
pub const PARQUET_COPY_FORMAT: &str =
    "FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE)";

/// Every `COPY` loads the files it names, whatever Snowflake's 64-day load history says, and stops at the first bad value.
pub const COPY_OPTIONS: &str = "ON_ERROR = ABORT_STATEMENT FORCE = TRUE";

/// Starts the error a direct `COPY` raises for a file it was not given, so a mismatch fails instead of loading wrong file names.
pub const UNEXPECTED_FILE_MARKER: &str = "sqlload: unexpected staged file ";

/// Characters a file key may hold for SQLLoad to name it in a stage path exactly.
pub const STAGE_KEY_SYMBOLS: &str = "._-=/+";
