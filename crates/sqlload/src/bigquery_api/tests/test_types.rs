pub(crate) struct JobAnswerTestCase {
    pub(crate) description: &'static str,
    pub(crate) response: serde_json::Value,
    /// The job still running, by id; `None` when it is done.
    pub(crate) expected_running: Option<&'static str>,
    pub(crate) expected_page_token: Option<&'static str>,
    pub(crate) expected_columns: &'static [&'static str],
    pub(crate) expected_rows: Vec<Vec<Option<&'static str>>>,
}

pub(crate) struct GoogleErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) body: &'static str,
    pub(crate) expected: (&'static str, &'static str),
}

pub(crate) struct KeyFileTestCase {
    pub(crate) description: &'static str,
    /// The key file's fields besides `private_key`, which every case gets from a key made for the test.
    pub(crate) fields: serde_json::Value,
    pub(crate) expected_error: &'static str,
    /// The assertion's claims, signed at `iat` 1000, when the key reads.
    pub(crate) expected_claims: serde_json::Value,
}
