pub(crate) struct ProfileCredentialsTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) profile: &'static str,
    /// Key id and whether a session token was found, or None when the profile has no keys.
    pub(crate) expected_keys: Option<(&'static str, bool)>,
}

pub(crate) struct KeySourceTestCase {
    pub(crate) description: &'static str,
    /// The connection's `profile`.
    pub(crate) profile: Option<&'static str>,
    /// `AWS_SHARED_CREDENTIALS_FILE`, in the test's folder: `credentials` holds the profiles, `missing` doesn't exist.
    pub(crate) credentials_file: &'static str,
    /// The rest of the environment.
    pub(crate) variables: &'static [(&'static str, &'static str)],
    /// Text where the keys come from, or the error, must contain.
    pub(crate) expected_text: &'static str,
}
