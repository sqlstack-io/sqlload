use crate::schema_plan::models::{ColumnChange, LiveColumn};

pub(crate) struct ChangeColumnsTestCase {
    pub(crate) description: &'static str,
    pub(crate) live: Vec<LiveColumn>,
    pub(crate) changes: Vec<ColumnChange>,
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct StaleRemovalsTestCase {
    pub(crate) description: &'static str,
    /// Each load folder's files, by their last-modified time (`None` for a file listed without one).
    pub(crate) loads: Vec<(&'static str, Vec<Option<i64>>)>,
    pub(crate) expected_files: Vec<&'static str>,
    pub(crate) expected_folders: Vec<&'static str>,
}
