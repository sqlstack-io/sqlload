pub(crate) struct LoaderCommandTestCase {
    pub(crate) description: &'static str,
    pub(crate) project: &'static str,
    /// Text the error must contain, or `""` when the command succeeds.
    pub(crate) expected_error: &'static str,
}

pub(crate) struct BlockingThreadsTestCase {
    pub(crate) description: &'static str,
    pub(crate) cli_threads: Option<u16>,
    pub(crate) config: &'static str,
    pub(crate) expected_threads: Option<usize>,
}

pub(crate) struct LoadOptionsTestCase {
    pub(crate) description: &'static str,
    pub(crate) argv: &'static [&'static str],
    /// `select | exclude | start | end`, or text the parse error must contain.
    pub(crate) expected_options: Result<&'static str, &'static str>,
}
