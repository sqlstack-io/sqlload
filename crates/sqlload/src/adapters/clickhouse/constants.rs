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

/// Settings an insert of rows also runs with, so it writes them before it returns: never through the server-side async buffer, which ClickHouse 26 turns on by default and a profile may set to return before the rows are written. Only inserts send it, so a profile that fixes `async_insert` fails only them, with ClickHouse's own message.
pub const INSERT_SETTINGS: &[(&str, &str)] = &[("async_insert", "0")];

/// How long one request may run: a statement can copy whole partitions, and an insert carries up to a batch of rows.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);

/// The header carrying the code of the error a request failed with.
pub const EXCEPTION_CODE_HEADER: &str = "X-ClickHouse-Exception-Code";

/// The header carrying a finished query's progress, such as the rows it wrote.
pub const SUMMARY_HEADER: &str = "X-ClickHouse-Summary";

/// The error codes of an `EXCHANGE TABLES` the database cannot run (a database not of the `Atomic` engine), which a rename swap replaces.
pub const EXCHANGE_UNSUPPORTED_CODES: &[&str] = &["1", "48"];

/// The error code of a lightweight `DELETE` the server or table refuses (lightweight deletes turned off, or a table with projections that is set to refuse them), which `ALTER TABLE ... DELETE` replaces.
pub const LIGHTWEIGHT_DELETE_REFUSED_CODE: &str = "344";

/// The setting a lightweight `DELETE` runs with, so it marks rows deleted in the parts that hold them: a patch part, as another mode writes, is not carried by `REPLACE PARTITION`. Servers before ClickHouse 25.5 have no such setting, and a profile may pin another value; there the copies are rebuilt from the kept rows instead.
pub const LIGHTWEIGHT_DELETE_MODE: (&str, &str) = ("lightweight_delete_mode", "alter_update");

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

/// The zstd level the rows of an insert are compressed with: the fastest, which costs less time than the bytes it saves take to send on a link slower than about 1 GB/s.
pub const INSERT_COMPRESSION_LEVEL: i32 = 1;

/// The most characters of an error response kept in the error.
pub const ERROR_PREVIEW: usize = 2_000;

/// The settings a pull reading `_sqlload_source_row` runs with, so each file's rows are numbered in the order they are stored.
pub const ORDERED_READ_SETTINGS: &str =
    "max_threads = 1, max_insert_threads = 1, input_format_parquet_preserve_order = 1";

/// Shown for `copy_to_internal_stage` on a ClickHouse target.
pub const NO_INTERNAL_STAGE_COPY: &str =
    "copy_to_internal_stage is for Snowflake targets; ClickHouse takes the rows as Arrow over HTTP";

/// The query parameters a pull's `s3()` takes its credentials from.
pub const KEY_ID_PARAMETER: &str = "sqlload_key_id";
pub const SECRET_PARAMETER: &str = "sqlload_secret";
pub const TOKEN_PARAMETER: &str = "sqlload_token";

/// The multipart boundary of a request that sends query parameters beside its SQL.
pub const FORM_BOUNDARY: &str = "sqlload-form-7c1e0b2f9a4d";

/// How `JSONCompactStrings` writes a NULL.
pub const NULL_TEXT: &str = "ᴺᵁᴸᴸ";
