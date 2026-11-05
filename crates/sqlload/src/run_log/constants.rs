/// The run log table, in the schema of the tables it records.
pub const RUNS_TABLE: &str = "_sqlload_runs";

/// The folder below a files table's folder holding its run records, beside its pending markers.
pub const RUNS_FOLDER: &str = "_sqlload/runs/";

/// The version of the record format, written with every record: 2 since a run may commit in batches, a record per commit (`batch`), which an older sqlload must not read as one record per run.
pub const RECORD_FORMAT_VERSION: u32 = 2;

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

/// How `recorded_at` is written: ISO 8601 UTC with microseconds, so text order is time order.
pub const RECORDED_AT_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";
