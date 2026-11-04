pub(crate) struct PostgresColumnTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: &'static str,
    /// The natural portable type the catalog maps the column to.
    pub(crate) expected_type: &'static str,
    /// Each row's value as Arrow displays it; empty for NULL.
    pub(crate) expected_values: &'static [&'static str],
}

pub(crate) struct LoadRunTestCase {
    pub(crate) description: &'static str,
    /// SQL run in Postgres before this load.
    pub(crate) source_change: &'static str,
    /// A query on the DuckDB destination and its expected single text result.
    pub(crate) check_query: &'static str,
    pub(crate) expected_result: &'static str,
}
