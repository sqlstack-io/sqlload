pub(crate) struct PostgresColumnTestCase {
    pub(crate) description: &'static str,
    pub(crate) column: &'static str,
    /// The natural portable type the catalog maps the column to.
    pub(crate) expected_type: &'static str,
    /// Each row's value as Arrow displays it; empty for NULL.
    pub(crate) expected_values: &'static [&'static str],
}
