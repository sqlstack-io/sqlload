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
