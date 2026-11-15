use serde::{Deserialize, Serialize};

/// One `sl.source(...)` (or a standalone `@sl.table`, as a source of its own) found under `python_paths`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonSource {
    pub name: String,
    /// The file that declares it, relative to the project directory.
    pub module: String,
    /// The source's settings with TOML key names, its tables under `tables` keyed by table name.
    pub settings: serde_json::Map<String, serde_json::Value>,
    /// The `sl.http` paths each table function writes as literals, by table name; tables that build no job are left out.
    #[serde(default)]
    pub http_paths: std::collections::BTreeMap<String, crate::http_source::models::LiteralPaths>,
}

/// What one table function is called with: its connection, the cursor window and the run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableRead {
    pub source: String,
    pub table: String,
    /// The source's connection name, if it names one.
    pub connection: Option<String>,
    /// The resolved settings of every connection no target writes to (and the source's own), by name; secrets included, as the table function needs them.
    pub connections: serde_json::Map<String, serde_json::Value>,
    /// Connections left out of `connections` because targets write to them.
    pub withheld: Vec<String>,
    pub cursor: Option<CursorRead>,
    /// Whether the table keeps its position in the run log, so `ctx.state` may be set.
    pub logged: bool,
    /// A logged table's position from its run log (`ctx.state`), as JSON text.
    pub state: Option<String>,
    pub is_reload: bool,
    /// Top-level objects the crossing expands one level into `<object>__<key>` columns.
    pub flatten: Vec<String>,
    /// Columns loaded masked.
    pub masked: Vec<String>,
    /// Columns pinned as `array(...)`: their Arrow lists cross as lists, so each element converts exactly.
    pub arrays: Vec<String>,
    /// How the table's columns load and the destination's name limit, which the crossing matches yielded fields to `cursor` and `arrays` by; not sent to the table function.
    #[serde(skip)]
    pub naming: (crate::load_naming::models::Naming, Option<usize>),
}

/// The cursor window a table function may use to read only new data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CursorRead {
    pub column: String,
    /// The cursor's portable type, such as `timestamp_tz` or `int`.
    pub column_type: String,
    /// Read from this value (the destination's maximum minus `lookback`, `cursor_start`, or `--start-cursor`); `None` reads everything.
    pub start: Option<String>,
    /// Read only values below this one (`--end-cursor`).
    pub end: Option<String>,
    /// Whether `start` itself is included.
    pub inclusive: bool,
}

/// What a `transform` gets as `ctx`: the unit its rows came from, `"file"`, `"range"` or `"page"`. A file sets `path`, `modified_at` and, for its format, `sheet`, `range` and `skip_rows`; a Google Sheets range sets `spreadsheet_id`, `sheet` (the tab) and `range`; an `sl.http` page sets `page` and, in a fan-out, `key`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TransformContext {
    pub unit: &'static str,
    /// The table's name in its source.
    pub table: String,
    /// The file as `_sqlload_source_file` names it.
    pub path: Option<String>,
    /// When the file was last modified, RFC 3339 in UTC.
    pub modified_at: Option<String>,
    pub sheet: Option<String>,
    pub range: Option<String>,
    pub skip_rows: Option<u32>,
    pub spreadsheet_id: Option<String>,
    /// The page's number within its request, from 1.
    pub page: Option<u64>,
    /// The fields of the fan-out key the page was requested for.
    pub key: Option<serde_json::Map<String, serde_json::Value>>,
}

/// One call of a `transform`, and how what it returned crosses.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransformCall {
    /// The name `@sl.transform` registered it under.
    pub function: String,
    pub context: TransformContext,
    /// Columns loaded masked.
    pub masked: Vec<String>,
    /// Columns pinned as `array(...)`: their Arrow lists cross as lists.
    pub arrays: Vec<String>,
    /// How the table's columns load and the destination's name limit.
    #[serde(skip)]
    pub naming: (crate::load_naming::models::Naming, Option<usize>),
}

/// How a `transform`'s call on one file or range went, once every batch it returned or yielded has crossed into Arrow and been passed on.
#[derive(Debug, Clone)]
pub struct TransformRan {
    /// Whether it returned dicts, which cross slower than Arrow or a DataFrame.
    pub dicts: bool,
    /// The rows it was given.
    pub rows_in: u64,
    /// How long the call and its crossing took, not counting the waits for its batches to be taken.
    pub seconds: f64,
}

/// How long a `transform` took on one file or Sheets range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransformTime {
    /// The file as `_sqlload_source_file` names it, or the range.
    pub file: String,
    /// How long the call, and its crossing into Arrow, took.
    pub milliseconds: u64,
    /// The rows the function was given.
    pub rows: u64,
    /// Whether it returned dicts, which cross slower than Arrow or a DataFrame.
    pub dicts: bool,
}

/// What `transform_files` runs: the host, the call (its `context.table` and how what the function returns crosses; each file sets the rest of the context), and the files, in the folder printed as `location`, encoded as `encoding`.
#[derive(Debug, Clone)]
pub struct TransformFiles<'a> {
    pub host: std::sync::Arc<dyn crate::python_sources::types::PythonHost>,
    pub call: TransformCall,
    pub files: &'a [crate::files_source::models::DataFile],
    pub location: String,
    pub encoding: &'a crate::files_source::models::FilesEncoding,
}
