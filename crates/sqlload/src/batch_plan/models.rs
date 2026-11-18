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
    /// The table's `timezone`, which a bound without an offset is read in while the cursor's type is unknown.
    pub zone: chrono_tz::Tz,
    /// The most windows the read may be split into (see [`crate::loader::models::TableSpec::most_windows`]).
    pub most: u64,
}

/// Files that commit together, in key order, and the cursor values their rows carry when they replace whole partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileGroup {
    /// Indexes into the files planned.
    pub files: Vec<usize>,
    /// The partition values of the cursor column the group replaces (`delete_insert`); empty otherwise.
    pub values: Vec<String>,
}

/// How a [`WindowPlan`] steps from one bound to the next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowStep {
    /// Whole days of a date cursor.
    Days(i64),
    /// A timestamp cursor's step in microseconds; `zoned` bounds end in `+00`.
    Micros {
        micros: i64,
        zoned: bool,
    },
    Integer(i128),
}

/// A read range split into `count` windows of `step`, each worked out when it is read, so a long range never holds its windows in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowPlan {
    /// The first window's start, as cursor text.
    pub start: String,
    /// Where the last window ends, as cursor text.
    pub end: String,
    pub step: WindowStep,
    /// Whether the first window reads its start; later windows always read theirs.
    pub inclusive: bool,
    pub count: u64,
}

/// The rows a Python table's function returned outside the cursor window it was asked for (a NULL cursor is `null_cursor`'s, not counted here): how many before the window's start and after its end, the earliest and latest of their cursor values, and the first of them.
#[derive(Debug, Clone, Default)]
pub struct OutsideRows {
    pub before: u64,
    pub after: u64,
    /// The earliest cursor value outside, as a one-value array of the cursor column's type.
    pub earliest: Option<arrow_array::ArrayRef>,
    /// The latest cursor value outside, likewise.
    pub latest: Option<arrow_array::ArrayRef>,
    /// The first row outside: its number among the rows the function returned for the window (from 1), and its cursor value.
    pub first: Option<(u64, arrow_array::ArrayRef)>,
}

impl OutsideRows {
    /// How many rows were outside.
    pub fn count(&self) -> u64 {
        self.before + self.after
    }
}

/// A batch's rows inside its cursor window (with those outside too, when they load), how many its bounds skipped for a NULL cursor, and the rows outside the window so far.
#[derive(Debug, Clone)]
pub struct WindowRows {
    pub kept: arrow_array::RecordBatch,
    pub nulls: usize,
    pub outside: OutsideRows,
}

/// Why rows outside a window likely came back, the first rule that fits winning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutsideCause {
    /// Every one of them is on the calendar day (in the table's zone) of the window's end, or, before a start after midnight, of its start: an API filtering by whole days.
    WholeDay { day: String, after: bool },
    /// Those before the start are on its day and those after the end on its day: an API filtering by whole days, asked for a window that starts after midnight.
    WholeDays { first: String, last: String },
    /// Every one of them is exactly at the bound the window leaves out: an API including it.
    AtBound { after: bool },
    /// Every one of them is within a few hours of one bound: the window sent in another time zone than the API reads it in, an offset of this many half hours.
    Offset { half_hours: i64, after: bool },
    /// Every one of them is after the end, none before the start: the end not sent, or ignored.
    NoEnd,
    /// Anything else.
    Unfiltered,
}
