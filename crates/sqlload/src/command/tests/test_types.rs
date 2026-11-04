pub(crate) struct LoaderCommandTestCase {
    pub(crate) description: &'static str,
    pub(crate) project: &'static str,
    /// Text the error must contain, or `""` when the command succeeds.
    pub(crate) expected_error: &'static str,
}
