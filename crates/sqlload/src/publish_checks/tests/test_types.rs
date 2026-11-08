use crate::project::models::{TableChecks, WriteStrategy};

pub(crate) struct CheckProblemsTestCase {
    pub(crate) description: &'static str,
    pub(crate) checks: TableChecks,
    pub(crate) strategy: WriteStrategy,
    pub(crate) files_target: bool,
    pub(crate) expected_problems: &'static [&'static str],
}

pub(crate) struct CheckColumnProblemsTestCase {
    pub(crate) description: &'static str,
    pub(crate) checks: TableChecks,
    pub(crate) loaded: &'static [&'static str],
    pub(crate) expected_problems: &'static [&'static str],
}
