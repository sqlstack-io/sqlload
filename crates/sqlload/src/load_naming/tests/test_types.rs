pub(crate) struct DltNameTestCase {
    pub(crate) description: &'static str,
    /// `dlt_identifier` or `dlt_table_path`.
    pub(crate) normalise: fn(&str, Option<usize>) -> String,
    pub(crate) name: &'static str,
    pub(crate) max_length: Option<usize>,
    pub(crate) expected_name: &'static str,
}
