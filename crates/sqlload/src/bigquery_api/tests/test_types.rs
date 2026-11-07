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

pub(crate) struct KeyFileTestCase {
    pub(crate) description: &'static str,
    /// The key file's fields besides `private_key`, which every case gets from a key made for the test.
    pub(crate) fields: serde_json::Value,
    /// The assertion's claims, signed at `iat` 1000, or why the key does not read.
    pub(crate) expected_claims: Result<serde_json::Value, &'static str>,
}
