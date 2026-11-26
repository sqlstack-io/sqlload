use serde::{Deserialize, Serialize};

use crate::row_batches::models::Flatten;

/// One `sl.source(...)` (or a standalone `@sl.table`, as a source of its own) found under `python_paths`, or one a host declares itself ([`PythonSource::new`]).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PythonSource {
    pub name: String,
    /// The file that declares it, relative to the project directory.
    pub module: String,
    /// The source's settings with TOML key names, its tables under `tables` keyed by table name.
    pub settings: serde_json::Map<String, serde_json::Value>,
    /// The `sl.http` paths each table function writes as literals, by table name; tables that build no job are left out.
    #[serde(default)]
    pub http_paths: std::collections::BTreeMap<String, crate::http_source::models::LiteralPaths>,
    /// The host's own data about the source (its loader, run or model), which the engine never reads and copies into every [`TableRead`] of the source's tables; SQLLoad's sources set none.
    #[serde(default)]
    pub host: Option<serde_json::Value>,
}

impl PythonSource {
    /// Source `name`, declared in `module` (as error messages name it), with its `settings` as TOML keys and its tables under `tables`; no literal `sl.http` paths and no host data.
    pub fn new(
        name: impl Into<String>,
        module: impl Into<String>,
        settings: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        Self {
            name: name.into(),
            module: module.into(),
            settings,
            http_paths: std::collections::BTreeMap::new(),
            host: None,
        }
    }
}

/// What one table function is called with: its connection, the cursor window and the run.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
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
    /// The objects the crossing expands into `<object>__<key>` columns.
    #[serde(skip)]
    pub flatten: Flatten,
    /// Columns loaded masked.
    pub masked: Vec<String>,
    /// Columns pinned as `array(...)`: their Arrow lists cross as lists, so each element converts exactly.
    pub arrays: Vec<String>,
    /// How the table's columns load and the destination's name limit, which the crossing matches yielded fields to `cursor` and `arrays` by; not sent to the table function.
    #[serde(skip)]
    pub naming: (crate::load_naming::models::Naming, Option<usize>),
    /// The table's `timezone`, which an `sl.field` parse without its own `tz` reads times without an offset in; `None` is UTC.
    #[serde(skip)]
    pub zone: Option<chrono_tz::Tz>,
    /// The most the table's `sl.http` jobs hold in pages requested ahead: a quarter of its share of the run's memory budget; `None` without a budget. Not sent to the table function.
    #[serde(skip)]
    pub source_part: Option<u64>,
    /// The source's [`PythonSource::host`] data, as the host declared it.
    pub host: Option<serde_json::Value>,
}

impl TableRead {
    /// A read of `table` from `source` with nothing else set: no connection, cursor or state, nothing masked or flattened; for a host's tests and its own reads.
    pub fn new(source: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            table: table.into(),
            connection: None,
            connections: serde_json::Map::new(),
            withheld: Vec::new(),
            cursor: None,
            logged: false,
            state: None,
            is_reload: false,
            flatten: Flatten::default(),
            masked: Vec::new(),
            arrays: Vec::new(),
            naming: (crate::load_naming::models::Naming::default(), None),
            zone: None,
            source_part: None,
            host: None,
        }
    }
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
    /// The table's `timezone` (an IANA name), which `ctx.cursor_value` and `ctx.cursor_end` arrive in; `None` is UTC.
    pub timezone: Option<String>,
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
    /// The objects of what it returns that expand into `<object>__<key>` columns (it is given the rows as read).
    #[serde(skip)]
    pub flatten: Flatten,
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
