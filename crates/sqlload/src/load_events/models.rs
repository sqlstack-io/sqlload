//! The load events and the stream that carries them.

use serde::{Deserialize, Serialize};

/// One thing a load did, as `--events` writes it: one JSON object per line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadEvent {
    pub schema_version: u32,
    pub run_id: String,
    /// When it happened, as RFC 3339 UTC.
    pub at: String,
    /// The destination table, named as `plan` names it; absent for the run as a whole and for a connection's rate limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// What happened, tagged by `event`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum EventKind {
    /// The load starts: how many tables it selected and will load.
    RunStarted { selected: usize, to_load: usize },
    /// A table starts loading, with its write strategy as `plan` shows it.
    TableStarted { strategy: String },
    /// A table finished part of its read: `unit` is `page`, `batch`, `window` or `file`, `count` how many so far, `rows` its rows so far.
    TableProgress { unit: String, count: u64, rows: u64 },
    /// Before the plan, a source's or target's connection opened, or failed to (`ok`), after `elapsed_ms`; `label` says what it reached, such as `OPENEDGE through ODBC`.
    Connected {
        role: Role,
        name: String,
        label: String,
        ok: bool,
        elapsed_ms: u64,
    },
    /// Before the plan, a source's tables were listed and described, or that failed (`ok`), after `elapsed_ms` past its connection.
    Discovered {
        source: String,
        tables: usize,
        ok: bool,
        elapsed_ms: u64,
    },
    /// A request waits its turn under a connection's `rate_limit` (`rate`, such as `5/s`).
    RateLimited {
        connection: String,
        rate: String,
        wait_ms: u64,
    },
    /// A table's rows are read and its last commit begins.
    TableCommitting { rows: u64 },
    /// Under `load --follow`, a batch of the followed source's changes was written: this table's rows in it, and how long ago the batch's newest change committed, in seconds.
    TableBatchWritten {
        rows: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        behind_secs: Option<u64>,
    },
    /// A table loaded: its rows, how long it took, and its new position when it keeps one.
    TableDone {
        rows: u64,
        elapsed_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<String>,
        /// The `created` and `altered` lines of a table that made or widened its columns as it loaded (a Python table, or a nested one's child tables).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changes: Vec<String>,
        /// How long a files or Sheets table's `transform` took on each file or range.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transforms: Vec<crate::python_sources::models::TransformTime>,
        /// A line per column `rename` named or numbering numbered: `Qty (column 3) -> qty_1 (numbered)`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        names: Vec<String>,
        /// What the load warns of, such as rows outside a cursor window.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
        /// The most memory the process held while the table loaded, in bytes; only under `--memory-per-table`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peak_memory_bytes: Option<u64>,
    },
    /// A table failed, and whether running the load again may get past it.
    TableFailed {
        reason: String,
        retryable: bool,
        elapsed_ms: u64,
        /// The most memory the process held while the table loaded, in bytes; only under `--memory-per-table`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peak_memory_bytes: Option<u64>,
    },
    /// A table the load never started, and why.
    TableNotStarted { reason: String },
    /// The load ended: its counts, the exit code, and the most memory the whole process held at once.
    RunFinished {
        loaded: usize,
        failed: usize,
        not_started: usize,
        rows: u64,
        elapsed_ms: u64,
        exit_code: u8,
        /// In bytes; absent where the OS gives no figure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peak_memory_bytes: Option<u64>,
    },
}

/// Whether a connection is a source's or a target's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Source,
    Target,
}

/// How one step before the plan ended: a connection opening, or failing to; a source's tables found (how many), or not (`None`); or a step that ends without a line of its own, such as planning a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepEnd {
    Connected { role: Role, label: String, ok: bool },
    Discovered { tables: Option<usize> },
    Quiet,
}

/// How long a connection or a source's discovery took, for the plan's report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PhaseTiming {
    pub role: Role,
    pub name: String,
    /// `connect` or `discover`.
    pub phase: &'static str,
    pub ok: bool,
    pub elapsed_ms: u64,
}

/// The step running now: whose it is, what it does, its id (each step's own), and how long it has run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepView {
    pub id: u64,
    pub subject: String,
    pub doing: String,
    pub elapsed: std::time::Duration,
}

/// What the plan's progress has to show: the phase lines finished since last asked, and the step running now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressView {
    pub lines: Vec<String>,
    pub step: Option<StepView>,
}
