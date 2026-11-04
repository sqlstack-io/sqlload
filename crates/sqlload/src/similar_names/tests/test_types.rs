pub(crate) struct CloseNameTestCase {
    pub(crate) description: &'static str,
    pub(crate) name: &'static str,
    pub(crate) candidates: &'static [&'static str],
    pub(crate) expected_close: Option<&'static str>,
}
