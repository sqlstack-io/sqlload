/// The run log table, in the schema of the tables it records.
pub const RUNS_TABLE: &str = "_sqlload_runs";

/// The folder below a files target's root holding one folder of run records per table.
pub const RUNS_FOLDER: &str = "_sqlload/runs/";

/// The version of the record format, written with every record.
pub const RECORD_FORMAT_VERSION: u32 = 1;

/// Snowflake time travel for the run log: the most Enterprise edition allows.
pub const SNOWFLAKE_RETENTION_DAYS: u32 = 90;

/// Snowflake time travel for the run log where the edition allows only one day.
pub const SNOWFLAKE_FALLBACK_RETENTION_DAYS: u32 = 1;

/// The file name suffix of a run record object.
pub const RECORD_SUFFIX: &str = ".json";

/// The run log's columns, all text: the record's fields for people and SQL, and the whole record as JSON for SQLLoad.
pub const RUNS_COLUMNS: &[&str] = &[
    "run_id",
    "event",
    "source",
    "source_table",
    "table_name",
    "cursor_column",
    "cursor_before",
    "cursor_after",
    "state",
    "row_count",
    "note",
    "recorded_at",
    "format_version",
    "record_json",
];

/// The statement opening a transaction.
pub const BEGIN_STATEMENT: &str = "BEGIN";

/// The statement committing a transaction.
pub const COMMIT_STATEMENT: &str = "COMMIT";

/// How a `MERGE` statement starts.
pub const MERGE_KEYWORD: &str = "MERGE";
