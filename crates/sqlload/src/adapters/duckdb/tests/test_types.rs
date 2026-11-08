use crate::adapters::duckdb::_helpers::pull_sql::PulledFile;
use crate::schema_plan::models::DesiredColumn;

pub(crate) struct PullInsertTestCase {
    pub(crate) description: &'static str,
    pub(crate) columns: Vec<DesiredColumn>,
    pub(crate) in_files: Vec<String>,
    pub(crate) files: Vec<PulledFile>,
    pub(crate) expected_sql: &'static str,
}

pub(crate) struct ReservedNameTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: &'static str,
    pub(crate) expected_refused: bool,
}
