/// The HTTP port of a connection that sets none.
pub const DEFAULT_HTTP_PORT: u16 = 8123;

/// The HTTPS port of a `secure` connection that sets none.
pub const DEFAULT_HTTPS_PORT: u16 = 8443;

/// Settings every request runs with, so SQLLoad's SQL behaves the same on every server: statements finish before the next starts, a NULL never becomes a default value, wall-clock timestamps read in UTC (as the Arrow values are sent), one insert may fill any number of partitions (a first load of a long history does), and errors come with the response's status.
pub const SESSION_SETTINGS: &[(&str, &str)] = &[
    ("mutations_sync", "2"),
    ("insert_null_as_default", "0"),
    ("input_format_null_as_default", "0"),
    ("session_timezone", "UTC"),
    ("date_time_input_format", "best_effort"),
    ("max_partitions_per_insert_block", "0"),
    ("cast_keep_nullable", "1"),
    ("join_use_nulls", "1"),
    ("wait_end_of_query", "1"),
];

/// How long one request may run: a statement can copy whole partitions, and an insert carries up to a batch of rows.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);

/// The header carrying the code of the error a request failed with.
pub const EXCEPTION_CODE_HEADER: &str = "X-ClickHouse-Exception-Code";

/// The error codes of an `EXCHANGE TABLES` the database cannot run (a database not of the `Atomic` engine), which a rename swap replaces.
pub const EXCHANGE_UNSUPPORTED_CODES: &[&str] = &["1", "48"];

/// The engine a created table gets when its layout names none.
pub const DEFAULT_ENGINE: &str = "MergeTree";

/// The sorting key of an unsorted table.
pub const EMPTY_SORTING_KEY: &str = "tuple()";

/// The table setting that lets a sorting, partitioning or primary key use `Nullable` columns, which every loaded column is.
pub const NULLABLE_KEY_SETTING: &str = "allow_nullable_key";

/// How a publish step starts: the step itself follows as JSON, and the statement runner carries it out instead of sending it.
pub const PUBLISH_PREFIX: &str = "-- sqlload publish ";

/// Suffix (after the delta table's name) of the copy a publish rebuilds partitions in.
pub const COPY_SUFFIX: &str = "_copy";

/// Suffix (after the build table's name) a rename swap moves the old table to.
pub const ASIDE_SUFFIX: &str = "_aside";

/// The most bytes of Arrow rows one insert request carries; a batch's rows go in several.
pub const INSERT_BYTES: usize = 64 * 1024 * 1024;

/// The most characters of an error response kept in the error.
pub const ERROR_PREVIEW: usize = 2_000;

/// Shown for `copy_to_internal_stage` on a ClickHouse target.
pub const NO_INTERNAL_STAGE_COPY: &str =
    "copy_to_internal_stage is for Snowflake targets; ClickHouse takes the rows as Arrow over HTTP";
