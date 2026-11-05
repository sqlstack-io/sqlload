/// The stage SQLLoad creates in each loader schema (stored upper-cased, `_SQLLOAD_STAGE`).
pub const MANAGED_STAGE_NAME: &str = "_sqlload_stage";

/// The most files one `COPY` names, Snowflake's limit for a `FILES` list.
pub const MAX_FILES_PER_COPY: usize = 1000;

/// The longest `COPY` statement built, below Snowflake's 1 MB limit on statement text; a batch of files whose statement would be longer is split.
pub const MAX_COPY_SQL_BYTES: usize = 900 * 1024;

/// How Snowflake reads the Parquet files.
pub const PARQUET_COPY_FORMAT: &str =
    "FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE)";

/// Every `COPY` loads the files it names, whatever Snowflake's 64-day load history says, and stops at the first bad value.
pub const COPY_OPTIONS: &str = "ON_ERROR = ABORT_STATEMENT FORCE = TRUE";

/// Starts the error a direct `COPY` raises for a file it was not given, so a mismatch fails instead of loading wrong file names.
pub const UNEXPECTED_FILE_MARKER: &str = "sqlload: unexpected staged file ";

/// Characters a file key may hold for SQLLoad to name it in a stage path exactly.
pub const STAGE_KEY_SYMBOLS: &str = "._-=/+";

/// Snowflake's codes for a missing privilege: `003001` is "Insufficient privileges".
pub const PRIVILEGE_CODES: &[&str] = &["003001"];

/// Snowflake's codes for an object that does not exist or that this role cannot see: `002003` for tables, schemas and stages, `002043` for operations on them.
pub const NOT_FOUND_CODES: &[&str] = &["002003", "002043"];

/// HTTP statuses saying a service refuses this role, or has no such object.
pub const REFUSING_HTTP_STATUSES: &[u16] = &[403, 404];

/// What a refusal says in lower case, whatever its code.
pub const REFUSAL_PHRASES: &[&str] = &["insufficient privileges", "not authorized"];
