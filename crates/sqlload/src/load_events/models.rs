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
    /// A request waits its turn under a connection's `rate_limit` (`rate`, such as `5/s`).
    RateLimited {
        connection: String,
        rate: String,
        wait_ms: u64,
    },
    /// A table's rows are read and its last commit begins.
    TableCommitting { rows: u64 },
    /// A table loaded: its rows, how long it took, and its new position when it keeps one.
    TableDone {
        rows: u64,
        elapsed_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<String>,
        /// The `created` and `altered` lines of a table that made or widened its columns as it loaded (a Python table, or a nested one's child tables).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changes: Vec<String>,
    },
    /// A table failed, and whether running the load again may get past it.
    TableFailed { reason: String, retryable: bool, elapsed_ms: u64 },
    /// A table the load never started, and why.
    TableNotStarted { reason: String },
    /// The load ended: its counts and the exit code.
    RunFinished {
        loaded: usize,
        failed: usize,
        not_started: usize,
        rows: u64,
        elapsed_ms: u64,
        exit_code: u8,
    },
}
