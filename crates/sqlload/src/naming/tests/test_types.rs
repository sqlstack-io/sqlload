pub(crate) struct ResolveTablesTestCase {
    pub(crate) description: &'static str,
    pub(crate) naming: &'static str,
    pub(crate) topics: &'static [&'static str],
    pub(crate) expected_tables: &'static [(&'static str, &'static str)],
}

pub(crate) struct ResolveTablesErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) naming: &'static str,
    pub(crate) topics: &'static [&'static str],
    pub(crate) expected_problem: &'static str,
}

pub(crate) struct ResolveStreamsTestCase {
    pub(crate) description: &'static str,
    pub(crate) naming: &'static str,
    pub(crate) topics: &'static [&'static str],
    pub(crate) expected_streams: &'static [(&'static str, &'static str, &'static str)],
}
