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
    pub is_reload: bool,
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
