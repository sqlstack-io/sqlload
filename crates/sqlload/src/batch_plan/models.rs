use chrono::{DateTime, Utc};

use crate::column_types::models::ColumnType;
use crate::project::models::BatchStep;

/// One cursor window a load reads and commits by itself: `from <= cursor < to` (`from < cursor` when not inclusive), each open when `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorWindow {
    pub from: Option<String>,
    pub to: Option<String>,
    pub inclusive: bool,
}

/// What splits a read range into windows of `step`.
#[derive(Debug, Clone, Copy)]
pub struct WindowSplit<'a> {
    pub from: Option<&'a str>,
    /// `--end-cursor-*`; without it, date and timestamp windows end now.
    pub to: Option<&'a str>,
    /// Whether the read's lower bound itself is read; later windows always read theirs.
    pub inclusive: bool,
    pub step: BatchStep,
    /// The cursor's type; `None` before a Python table's first load shows it, when the start's text decides.
    pub cursor_type: Option<&'a ColumnType>,
    pub now: DateTime<Utc>,
}

/// Files that commit together, in key order, and the cursor values their rows carry when they replace whole partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileGroup {
    /// Indexes into the files planned.
    pub files: Vec<usize>,
    /// The partition values of the cursor column the group replaces (`delete_insert`); empty otherwise.
    pub values: Vec<String>,
}
