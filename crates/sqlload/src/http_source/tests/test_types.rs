pub(crate) struct JobProblemsTestCase {
    pub(crate) description: &'static str,
    pub(crate) job: &'static str,
    pub(crate) expected_problems: &'static [&'static str],
}

pub(crate) struct UrlsTestCase {
    pub(crate) description: &'static str,
    pub(crate) value: fn() -> String,
    pub(crate) expected_value: &'static str,
}

pub(crate) struct RenewalTestCase {
    pub(crate) description: &'static str,
    pub(crate) expires_in: &'static str,
    pub(crate) expected_delay: Result<Option<std::time::Duration>, &'static str>,
}
