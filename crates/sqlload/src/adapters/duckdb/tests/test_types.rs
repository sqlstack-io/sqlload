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

pub(crate) struct UnloadedInsertTestCase {
    pub(crate) description: &'static str,
    /// The timestamp, the timestamp with time zone and the time, as whole microseconds.
    pub(crate) micros: [i64; 3],
    /// The session's time zone, with ICU loaded; `None` leaves ICU unloaded.
    pub(crate) time_zone: Option<&'static str>,
    pub(crate) expected_values: [&'static str; 6],
}

pub(crate) struct PulledUnloadedTestCase {
    pub(crate) description: &'static str,
    /// The rows the unload reports.
    pub(crate) reported: u64,
    /// Whether the unload wrote its file.
    pub(crate) with_files: bool,
    pub(crate) expected_outcome: &'static str,
    pub(crate) expected_rows: u64,
}

pub(crate) struct MotherDuckPathTestCase {
    pub(crate) description: &'static str,
    pub(crate) database: &'static str,
    pub(crate) expected: Result<&'static str, &'static str>,
}

pub(crate) struct MotherDuckTokenTestCase {
    pub(crate) description: &'static str,
    pub(crate) token: Option<&'static str>,
    /// The environment variables set, by name.
    pub(crate) variables: &'static [(&'static str, &'static str)],
    pub(crate) expected: Option<&'static str>,
}

pub(crate) struct MotherDuckRefusalTestCase {
    pub(crate) description: &'static str,
    /// DuckDB's error opening the database.
    pub(crate) error: String,
    pub(crate) token: &'static str,
    pub(crate) expected: &'static str,
}
