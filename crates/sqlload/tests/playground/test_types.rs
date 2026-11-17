/// Sections of the quickstart page to run, in order, in one folder.
pub(crate) struct QuickstartPageTestCase {
    pub(crate) description: &'static str,
    /// The sections' headings, without their `## `.
    pub(crate) headings: &'static [&'static str],
    /// The output blocks that differ from the run.
    pub(crate) expected_differences: &'static [&'static str],
}
