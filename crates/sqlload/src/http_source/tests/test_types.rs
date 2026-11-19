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

pub(crate) struct LimiterTestCase {
    pub(crate) description: &'static str,
    /// Which of the first four waiting requests are aborted while they wait.
    pub(crate) aborted: &'static [usize],
    /// How many requests went.
    pub(crate) expected_turns: usize,
}

pub(crate) struct CredentialSourceTestCase {
    pub(crate) description: &'static str,
    /// The connection's `auth`, as TOML.
    pub(crate) auth: &'static str,
    /// The credential settings the connection has.
    pub(crate) set: &'static [&'static str],
    pub(crate) expected_source: Result<&'static str, &'static str>,
}
