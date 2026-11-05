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
