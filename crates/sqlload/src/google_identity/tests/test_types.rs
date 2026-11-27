/// A connection's key and impersonated account, and the credentials file `GOOGLE_APPLICATION_CREDENTIALS` names.
pub(crate) struct CredentialsTestCase<'a> {
    pub(crate) description: &'static str,
    pub(crate) key: Option<&'static str>,
    pub(crate) impersonate: Option<&'static str>,
    pub(crate) named: Option<&'a str>,
    /// The start of `describe()`; empty when it fails.
    pub(crate) expected_description: &'static str,
    pub(crate) expected_project: Option<&'static str>,
    /// Text the error contains; empty when the credentials are found.
    pub(crate) expected_error: &'static str,
}

/// A key file, signed into an assertion.
pub(crate) struct KeyFileTestCase {
    pub(crate) description: &'static str,
    /// The key file's fields besides `private_key`, which every case gets from a key made for the test.
    pub(crate) fields: serde_json::Value,
    /// The assertion's claims, signed at `iat` 1000, or why the key does not read.
    pub(crate) expected_claims: Result<serde_json::Value, &'static str>,
}
