use serde::{Deserialize, Serialize};

/// What a run log record says happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEvent {
    /// A load that succeeded; its position becomes the table's state.
    Run,
    /// A load with `--start/--end-cursor-*`; recorded, but it does not move the state.
    Backfill,
    /// `sqlload state reset`: the next run reads from this position.
    Reset,
    /// The data's position replaced a logged one that disagreed with it.
    Reconcile,
}

impl std::fmt::Display for RunEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Run => "run",
            Self::Backfill => "backfill",
            Self::Reset => "reset",
            Self::Reconcile => "reconcile",
        })
    }
}

/// One record of the run log; never changed once written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub format_version: u32,
    /// The load id of a run (its rows carry it in `_sqlload_load_id`), or a fresh id for other events.
    pub run_id: String,
    pub event: RunEvent,
    pub source: String,
    /// The table's name in its source (the Python table).
    pub source_table: String,
    /// The destination table's name within its schema or target.
    pub table_name: String,
    pub cursor_column: Option<String>,
    /// Where the run read from.
    pub cursor_before: Option<String>,
    /// The position after the run: the data's cursor maximum.
    pub cursor_after: Option<String>,
    /// The table function's own position (`ctx.state`), as JSON text.
    pub state: Option<String>,
    pub row_count: u64,
    /// Files written (files targets).
    pub files: Vec<String>,
    pub note: Option<String>,
    /// When the record was made: ISO 8601 UTC with microseconds, so text order is time order.
    pub recorded_at: String,
    /// The run's commit this record went with, from 1, when the run committed in batches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<u64>,
}

/// What a finished run adds to its record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOutcome {
    /// The load id of the run.
    pub run_id: String,
    pub recorded_at: String,
    pub rows: u64,
    /// The largest cursor among the rows written.
    pub loaded_maximum: Option<String>,
    pub files: Vec<String>,
}

/// The table a record is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTable {
    pub source: String,
    pub source_table: String,
    pub table_name: String,
    pub cursor_column: Option<String>,
}

/// An event that is not a run (a reset or reconciliation): its position and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateEvent {
    pub event: RunEvent,
    pub cursor_before: Option<String>,
    pub cursor_after: Option<String>,
    pub state: Option<String>,
    pub note: String,
    pub recorded_at: String,
}

/// A table's run log as read: the records that parse, oldest first, and those that do not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodedRuns {
    pub records: Vec<RunRecord>,
    pub unreadable: Vec<UnreadableRecord>,
}

/// A record that cannot be used: it does not parse, or a newer SQLLoad wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableRecord {
    /// Its run id and time, when they can still be read.
    pub run_id: Option<String>,
    pub recorded_at: Option<String>,
    pub reason: String,
}

/// What creates a run log table that does not exist yet: its schema, the table, and a fallback for the table when the warehouse refuses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunsSetup {
    pub schema: String,
    pub create: String,
    pub fallback: Option<String>,
}
