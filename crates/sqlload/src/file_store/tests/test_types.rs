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

pub(crate) struct KeyedBuilderTestCase {
    pub(crate) description: &'static str,
    /// The process environment.
    pub(crate) variables: &'static [(&'static str, &'static str)],
    /// The static keys the chain chose, with their session token.
    pub(crate) keys: Option<(&'static str, Option<&'static str>)>,
    /// The client's key ID, session token and region.
    pub(crate) expected_config: [Option<&'static str>; 3],
}

pub(crate) struct GcsCredentialsTestCase {
    pub(crate) description: &'static str,
    /// Whether the connection sets `service_account_key`.
    pub(crate) has_key: bool,
    /// `GOOGLE_APPLICATION_CREDENTIALS`, in the test's folder: `sa.json` exists, `gone.json` doesn't.
    pub(crate) named: Option<&'static str>,
    /// The home folder, in the test's folder: `home` holds gcloud's file, `nobody` doesn't exist.
    pub(crate) home: Option<&'static str>,
    /// Text where the credentials come from, or the error, must contain.
    pub(crate) expected_text: &'static str,
    /// Whether the client builder takes those credentials, rather than failing with the error.
    pub(crate) expected_builds: bool,
}
