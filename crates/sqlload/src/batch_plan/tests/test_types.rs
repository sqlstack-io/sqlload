use crate::column_types::models::ColumnType;
use crate::project::models::BatchStep;

pub(crate) struct SplitWindowsTestCase {
    pub(crate) description: &'static str,
    pub(crate) from: Option<&'static str>,
    pub(crate) to: Option<&'static str>,
    pub(crate) inclusive: bool,
    pub(crate) step: BatchStep,
    pub(crate) cursor_type: Option<ColumnType>,
    /// The table's `timezone`, which a bound without an offset is read in while the cursor's type is unknown.
    pub(crate) zone: chrono_tz::Tz,
    /// The most windows allowed.
    pub(crate) most: u64,
    /// Each window as `from..to` (`(` when the start is not read), or the error.
    pub(crate) expected_windows: Result<&'static [&'static str], &'static str>,
}

pub(crate) struct FileGroupsTestCase {
    pub(crate) description: &'static str,
    /// The files listed, in key order.
    pub(crate) partitions: Vec<PartitionFiles>,
    pub(crate) by_value: Option<&'static str>,
    /// Each group's file count and the values it replaces.
    pub(crate) expected_groups: &'static [(usize, &'static [&'static str])],
}

pub(crate) struct CutIndexTestCase {
    pub(crate) description: &'static str,
    pub(crate) values: &'static [i64],
    pub(crate) last: Option<&'static str>,
    pub(crate) expected_cut: usize,
    pub(crate) expected_last: Option<&'static str>,
}

/// Some equal files of one `region` and `day`.
pub(crate) struct PartitionFiles {
    pub(crate) region: &'static str,
    pub(crate) day: &'static str,
    pub(crate) files: usize,
    pub(crate) file_bytes: u64,
}

pub(crate) struct BoundArrayTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) cursor_type: ColumnType,
    /// The bound as Arrow displays it.
    pub(crate) expected_value: &'static str,
}

pub(crate) struct WindowRowsTestCase {
    pub(crate) description: &'static str,
    /// Each row's cursor, in hours from 2026-03-01 00:00 UTC (`None` for NULL).
    pub(crate) hours: &'static [Option<i64>],
    /// Where the rows split into a second batch of the same window.
    pub(crate) split_at: usize,
    pub(crate) from: Option<&'static str>,
    pub(crate) to: Option<&'static str>,
    pub(crate) inclusive: bool,
    /// `null_cursor = "include"`.
    pub(crate) include: bool,
    pub(crate) rule: crate::project::models::OnCursorOutOfRange,
    /// The table's `timezone`, which the bounds are read in and times are shown in.
    pub(crate) zone: chrono_tz::Tz,
    /// How many rows load, across both batches.
    pub(crate) expected_kept: usize,
    /// How many rows the bounds skipped for a NULL cursor.
    pub(crate) expected_nulls: usize,
    /// What the load says of the rows outside (empty when there are none).
    pub(crate) expected_text: &'static str,
}

/// What `read_in_order` did in a case: the results consumed; the most reads running at once, the most results held at once and the first read to finish; and the error.
pub(crate) type InOrderOutcome = (Vec<u64>, (usize, usize, u64), Option<String>);

pub(crate) struct ReadInOrderTestCase {
    pub(crate) description: &'static str,
    pub(crate) count: u64,
    pub(crate) limit: usize,
    /// The read that fails, if any; each read takes longer the earlier its index, so later reads finish first.
    pub(crate) failing_read: Option<u64>,
    /// The result whose consumption fails, if any.
    pub(crate) failing_consume: Option<u64>,
    /// The results consumed, in order; the most reads running at once, the most results held at once and the first read to finish; and the error.
    pub(crate) expected_outcome: (&'static [u64], (usize, usize, u64), Option<&'static str>),
}
