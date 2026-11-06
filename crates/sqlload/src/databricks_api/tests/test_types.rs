pub(crate) struct StatementResponseTestCase {
    pub(crate) description: &'static str,
    pub(crate) response: serde_json::Value,
    /// The statement to poll, or the error it failed with; `None` when it finished.
    pub(crate) expected_state: Result<Option<&'static str>, &'static str>,
    pub(crate) expected_rows: Vec<Vec<Option<&'static str>>>,
}
