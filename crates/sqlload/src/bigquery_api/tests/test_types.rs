use crate::bigquery_api::_helpers::jobs::JobState;

pub(crate) struct JobAnswerTestCase {
    pub(crate) description: &'static str,
    pub(crate) response: serde_json::Value,
    pub(crate) expected_state: JobState,
    pub(crate) expected_columns: &'static [&'static str],
    pub(crate) expected_rows: Vec<Vec<Option<&'static str>>>,
}

pub(crate) struct GoogleErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) body: &'static str,
    pub(crate) expected_reason: &'static str,
    pub(crate) expected_message: &'static str,
}
