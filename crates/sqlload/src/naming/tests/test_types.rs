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

pub(crate) struct ResolveStreamsErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) naming: &'static str,
    pub(crate) topics: &'static [&'static str],
    pub(crate) expected_problem: &'static str,
}

pub(crate) struct QualifyStageTestCase {
    pub(crate) description: &'static str,
    pub(crate) stage: &'static str,
    pub(crate) expected_name: &'static str,
}

pub(crate) struct ShardTestCase {
    pub(crate) description: &'static str,
    pub(crate) before: u32,
    pub(crate) after: u32,
    /// The most streams out of 1,000 allowed to change owner.
    pub(crate) expected_max_moved: usize,
}
