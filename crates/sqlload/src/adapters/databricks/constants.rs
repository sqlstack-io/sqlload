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

/// How a batch's files are read where they lie in S3, after its `FILES` list: every column any of them holds (one lacking it gives NULL), every file again, even one a `COPY INTO` has loaded before.
pub const S3_COPY_OPTIONS: &str =
    "FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS ('force' = 'true')";

/// The external locations the service principal can see.
pub const LIST_LOCATIONS: &str = "SHOW EXTERNAL LOCATIONS";

/// The column of [`LIST_LOCATIONS`] holding each location's URL.
pub const LOCATION_URL_COLUMN: &str = "url";

/// The column of a `COPY INTO`'s result holding the rows it inserted.
pub const INSERTED_ROWS_COLUMN: &str = "num_inserted_rows";

/// Why `plan` refuses Parquet files in S3 that no external location covers, after their folder.
pub const OUTSIDE_LOCATIONS: &str = "cannot be read by Databricks: no Unity Catalog external location covers it, and Parquet files in S3 never go through SQLLoad into Databricks. Fix: create an external location whose URL holds the folder (CREATE EXTERNAL LOCATION ... URL 's3://bucket/folder/' WITH (STORAGE CREDENTIAL ...)), and grant the service principal READ FILES on it";

/// Shown for `copy_to_internal_stage` on a Databricks target.
pub const NO_INTERNAL_STAGE_COPY: &str = "copy_to_internal_stage is for Snowflake targets; Databricks loads the files through its staging volume";
