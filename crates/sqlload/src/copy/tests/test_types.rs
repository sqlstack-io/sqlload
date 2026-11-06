pub(crate) struct CopyProjectTestCase {
    pub(crate) description: &'static str,
    pub(crate) source: &'static str,
    pub(crate) destination: &'static str,
    pub(crate) project: &'static str,
    pub(crate) expected_lines: &'static [&'static str],
}
