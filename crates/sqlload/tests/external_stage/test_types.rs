use sqlload_testkit::parity::models::ParityCase;

pub(crate) struct ParityTestCase {
    pub(crate) description: &'static str,
    pub(crate) case: ParityCase,
    /// `same rows`, or `both fail`.
    pub(crate) expected_outcome: &'static str,
}

pub(crate) struct ComparisonTestCase {
    pub(crate) description: &'static str,
    pub(crate) tables: (&'static str, &'static str),
    pub(crate) expected_counts: &'static str,
}
