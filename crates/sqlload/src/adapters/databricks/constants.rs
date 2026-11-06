/// The table property letting Delta widen a column's type in place.
pub const TYPE_WIDENING: &str = "'delta.enableTypeWidening' = 'true'";

/// The table feature a `TIMESTAMP_NTZ` column added to an existing Delta table needs.
pub const TIMESTAMP_NTZ_FEATURE: &str = "'delta.feature.timestampNtz' = 'supported'";

/// How Databricks spells a timestamp without a time zone.
pub const TIMESTAMP_NTZ: &str = "TIMESTAMP_NTZ";

/// The folder in the staging volume holding each load's files, `sqlload/<load id>/<batch>/`.
pub const STAGE_FOLDER: &str = "sqlload";

/// Files of one batch uploading at once.
pub const CONCURRENT_UPLOADS: usize = 4;

/// Hours after which a load folder left in the volume is a failed run's, removed at the start of the next load.
pub const STALE_STAGE_HOURS: i64 = 24;

/// Statements the Statement Execution API cannot run as a transaction: each request commits on its own.
pub const TRANSACTION_STATEMENTS: &[&str] = &["BEGIN", "COMMIT", "ROLLBACK"];

/// How staged Parquet files are read: every file again, even one a `COPY INTO` has loaded before.
pub const COPY_OPTIONS: &str = "FILEFORMAT = PARQUET COPY_OPTIONS ('force' = 'true')";

/// Shown for `copy_to_internal_stage` on a Databricks target.
pub const NO_INTERNAL_STAGE_COPY: &str = "copy_to_internal_stage is for Snowflake targets; Databricks loads the files through its staging volume";
