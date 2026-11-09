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
    pub(crate) expected_path: Result<&'static str, &'static str>,
}

pub(crate) struct MotherDuckTokenTestCase {
    pub(crate) description: &'static str,
    pub(crate) token: Option<&'static str>,
    /// The environment variables set, by name.
    pub(crate) variables: &'static [(&'static str, &'static str)],
    pub(crate) expected_token: Option<&'static str>,
}

pub(crate) struct ConnectionStringTestCase {
    pub(crate) description: &'static str,
    pub(crate) path: &'static str,
    pub(crate) token: &'static str,
    pub(crate) expected_string: &'static str,
}

pub(crate) struct MotherDuckRefusalTestCase {
    pub(crate) description: &'static str,
    /// DuckDB's error opening the database.
    pub(crate) error: String,
    pub(crate) token: &'static str,
    pub(crate) expected_message: &'static str,
}

pub(crate) struct MotherDuckOpenTestCase {
    pub(crate) description: &'static str,
    pub(crate) connection_database: Option<&'static str>,
    pub(crate) target_database: Option<&'static str>,
    pub(crate) expected_error: &'static str,
}

pub(crate) struct ResolvedDatabaseTestCase {
    pub(crate) description: &'static str,
    /// The connection's `database` line, if any.
    pub(crate) connection_line: &'static str,
    /// The target's `database` line, if any.
    pub(crate) target_line: &'static str,
    /// The tables' database, or the project error.
    pub(crate) expected_database: Result<&'static str, &'static str>,
}

pub(crate) struct DuckDbPathTestCase {
    pub(crate) description: &'static str,
    pub(crate) path: &'static str,
    pub(crate) expected_error: Option<&'static str>,
}

pub(crate) struct MotherDuckRouteTestCase {
    pub(crate) description: &'static str,
    pub(crate) motherduck: bool,
    pub(crate) expected_reads_files: bool,
}

pub(crate) struct AttachedDatabaseTestCase {
    pub(crate) description: &'static str,
    /// Run before reading the schema, beside `raw.orders (id)` in the current database.
    pub(crate) setup: &'static str,
    pub(crate) expected_columns: &'static [&'static str],
    pub(crate) expected_dropped: usize,
}
