pub(crate) struct RenderedTestCase {
    pub(crate) description: &'static str,
    pub(crate) rendered: Vec<String>,
    pub(crate) expected: &'static [&'static str],
}
