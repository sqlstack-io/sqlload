use crate::column_types::models::ColumnType;
use crate::project::models::BatchStep;

pub(crate) struct SplitWindowsTestCase {
    pub(crate) description: &'static str,
    pub(crate) from: Option<&'static str>,
    pub(crate) to: Option<&'static str>,
    pub(crate) inclusive: bool,
    pub(crate) step: BatchStep,
    pub(crate) cursor_type: Option<ColumnType>,
    /// Each window as `from..to` (`(` when the start is not read), or the error.
    pub(crate) expected_windows: Result<&'static [&'static str], &'static str>,
}

pub(crate) struct FileGroupsTestCase {
    pub(crate) description: &'static str,
    /// Files per `day` partition, in key order.
    pub(crate) partitions: &'static [(&'static str, usize)],
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
