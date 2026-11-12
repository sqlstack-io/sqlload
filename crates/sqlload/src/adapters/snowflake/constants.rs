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

/// The longest text Snowflake holds, which `DESCRIBE TABLE` reports as the length of an unbounded `VARCHAR`.
pub const MAX_TEXT_LENGTH: i64 = 16_777_216;

/// Statements that start this way run in a transaction of their own, unlike Snowflake's DDL, which commits by itself.
pub const MERGE_KEYWORD: &str = "MERGE";

/// Time travel for the run log: the most Enterprise edition allows.
pub const RUN_LOG_RETENTION_DAYS: u32 = 90;

/// Time travel for the run log where the edition allows only one day.
pub const RUN_LOG_FALLBACK_RETENTION_DAYS: u32 = 1;

/// The stage SQLLoad creates in each loader schema (stored upper-cased, `_SQLLOAD_STAGE`).
pub const MANAGED_STAGE_NAME: &str = "_sqlload_stage";

/// The most files one `COPY` names, Snowflake's limit for a `FILES` list.
pub const MAX_FILES_PER_COPY: usize = 1000;

/// The longest `COPY` statement built, below Snowflake's 1 MB limit on statement text; a batch of files whose statement would be longer is split.
pub const MAX_COPY_SQL_BYTES: usize = 900 * 1024;

/// The longest multi-statement request a hashed batch sends at once (1.63 MB was accepted live); a longer one is split over several requests through a table of hashed values.
pub const MAX_SCRIPT_BYTES: usize = 1536 * 1024;

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

/// Digits after the second Snowflake's times and timestamps hold at most, which SQLLoad's new columns use.
pub const MAX_FRACTION_DIGITS: u8 = 9;

/// An RFC 3339 timestamp with an offset, as SQLLoad's JSON inference recognises one (`T`, `t` or a space between date and time).
pub const RFC3339_PATTERN: &str = "[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt ][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})";

/// The largest and smallest integers SQLLoad's JSON inference reads as `int`; others are text.
pub const MAX_JSON_INT: &str = "9223372036854775807";
pub const MIN_JSON_INT: &str = "-9223372036854775808";

/// Starts the error a direct `COPY` raises for a value that does not fit its column's inferred type.
pub const MISFIT_MARKER: &str = "sqlload: ";

/// The temporary file format a JSON batch's key query reads the stage with, in the loader schema; it goes with the session.
pub const KEYS_FILE_FORMAT: &str = "_SQLLOAD_KEYS";

/// The most staged files of one table uploading at once.
pub const CONCURRENT_UPLOADS: usize = 4;

/// The warehouse's name in messages.
pub const SNOWFLAKE: &str = "Snowflake";

/// The `ENCODING` a direct `COPY` reads CSV with, for each charset SQLLoad's reader decodes the same way, by its Encoding Standard name; other charsets are refused for the direct route.
pub const SNOWFLAKE_ENCODINGS: [(&str, &str); 15] = [
    ("windows-1250", "WINDOWS1250"),
    ("windows-1251", "WINDOWS1251"),
    ("windows-1252", "WINDOWS1252"),
    ("windows-1253", "WINDOWS1253"),
    ("windows-1254", "WINDOWS1254"),
    ("windows-1255", "WINDOWS1255"),
    ("windows-1256", "WINDOWS1256"),
    ("ISO-8859-2", "ISO88592"),
    ("ISO-8859-5", "ISO88595"),
    ("ISO-8859-6", "ISO88596"),
    ("ISO-8859-7", "ISO88597"),
    ("ISO-8859-15", "ISO885915"),
    ("KOI8-R", "KOI8R"),
    ("UTF-16LE", "UTF16LE"),
    ("UTF-16BE", "UTF16BE"),
];

/// Snowflake's name for UTF-8, the `ENCODING` a direct `COPY` of CSV leaves unsaid.
pub const UTF8_ENCODING: &str = "UTF8";

/// Snowflake's name for ISO-8859-1, which `latin-1` reads byte for byte.
pub const LATIN1_ENCODING: &str = "ISO88591";
