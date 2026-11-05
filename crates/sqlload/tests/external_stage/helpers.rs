use std::collections::BTreeMap;

use sqlload_testkit::parity::main::run_cases::run_cases;
use sqlload_testkit::parity::models::ParityCase;

use crate::test_types::ParityTestCase;

/// Every case's outcome by name, all of them run before any is checked.
pub(crate) fn all_outcomes(
    test_cases: &[ParityTestCase],
) -> Result<BTreeMap<String, String>, String> {
    let cases: Vec<ParityCase> = test_cases.iter().map(|test_case| test_case.case).collect();
    run_cases(&cases)
}
