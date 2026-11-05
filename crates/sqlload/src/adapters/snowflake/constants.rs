/// The stage loads write their files to: the user's own stage, which needs no grants.
pub const USER_STAGE: &str = "~";

/// Folder on the stage for one load's files.
pub const STAGE_FOLDER: &str = "sqlload";

/// Seconds a loader statement may run: 0, Snowflake's maximum, since one `COPY` or `INSERT` can move a whole table.
pub const LOADER_STATEMENT_TIMEOUT_SECS: u64 = 0;

/// The schema sessions start in; every statement names its objects fully.
pub const SESSION_SCHEMA: &str = "PUBLIC";

/// How staged Parquet files are read.
pub const COPY_FILE_FORMAT: &str =
    "FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE)";

/// The most files one `COPY` names, Snowflake's limit for a `FILES` list.
pub const MAX_COPY_FILES: usize = 1000;

/// Hours after which a load folder left on the user stage is a failed run's, removed at the start of the next load.
pub const STALE_STAGE_HOURS: i64 = 24;

/// The folder under an external stage's URL holding each load's staged Parquet.
pub const EXTERNAL_STAGE_FOLDER: &str = "_sqlload";

/// How long a stream's rows may take to commit, in seconds.
pub const STREAM_COMMIT_WAIT_SECS: u64 = 600;

/// How often a stream's commit is checked, in milliseconds.
pub const STREAM_POLL_MILLIS: u64 = 500;

/// The uncompressed bytes of NDJSON sent in one streaming request.
pub const STREAM_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// The pipe Snowflake provides for streaming into a table, after its name.
pub const STREAM_PIPE_SUFFIX: &str = "-STREAMING";

/// The temporary table probing whether table stages work for this role.
pub const STAGE_PROBE_TABLE: &str = "__sqlload_stage_probe";
