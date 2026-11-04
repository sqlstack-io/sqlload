/// The stage loads write their files to: the user's own stage, which needs no grants.
pub const USER_STAGE: &str = "~";

/// Folder on the stage for one load's files.
pub const STAGE_FOLDER: &str = "sqlload";

/// Seconds a loader statement may run: 0, Snowflake's maximum, since one `COPY` or `INSERT` can move a whole table.
pub const LOADER_STATEMENT_TIMEOUT_SECS: u64 = 0;

/// The schema sessions start in; every statement names its objects fully.
pub const SESSION_SCHEMA: &str = "PUBLIC";
