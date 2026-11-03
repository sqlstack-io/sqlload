pub(crate) struct ExpiredDatesTestCase {
    pub(crate) description: &'static str,
    pub(crate) retain_days: u64,
    pub(crate) lookback_days: u64,
    pub(crate) expected_dates: &'static [&'static str],
}

pub(crate) struct RenderFolderTestCase {
    pub(crate) description: &'static str,
    pub(crate) template: &'static str,
    pub(crate) expected_folder: &'static str,
}

pub(crate) struct ProfileCredentialsTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) profile: &'static str,
    /// Key id and whether a session token was found, or None when the profile has no keys.
    pub(crate) expected_keys: Option<(&'static str, bool)>,
}
