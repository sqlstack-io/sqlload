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
