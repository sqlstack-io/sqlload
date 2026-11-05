//! Run the parity cases against the live account.

use std::collections::BTreeMap;
use std::io::Write;

use crate::parity::_helpers::compare::run_case;
use crate::parity::_helpers::live_settings::live_settings;
use crate::parity::models::ParityCase;

/// Each case's outcome by name (`same rows`, `both fail`, or what differed), all of them run and printed, so one run shows everything to remove from the direct route; none without `SQLLOAD_PARITY_ACCOUNT`, which skips the test; `SQLLOAD_PARITY_ONLY` (names, comma-separated) runs just those cases.
pub fn run_cases(cases: &[ParityCase]) -> Result<BTreeMap<String, String>, String> {
    let mut outcomes: BTreeMap<String, String> = BTreeMap::new();
    let Some(live) = live_settings()? else {
        let _ = writeln!(
            std::io::stdout(),
            "skipped: SQLLOAD_PARITY_ACCOUNT is not set"
        );
        return Ok(outcomes);
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let only = std::env::var("SQLLOAD_PARITY_ONLY").unwrap_or_default();
    let chosen =
        |case: &&ParityCase| only.is_empty() || only.split(',').any(|name| name == case.name);
    for case in cases.iter().filter(chosen) {
        let outcome = match run_case(&runtime, &live, case) {
            Ok(outcome) => outcome,
            Err(reason) => format!("the case could not run: {reason}"),
        };
        let _ = writeln!(std::io::stdout(), "{}: {outcome}", case.name);
        outcomes.insert(case.name.to_owned(), outcome);
    }
    Ok(outcomes)
}
