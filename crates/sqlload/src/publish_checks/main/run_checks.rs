//! Run a table's checks on the rows a commit is about to publish.

use crate::loader::errors::LoaderError;
use crate::project::models::TableChecks;
use crate::publish_checks::_helpers::failures::failures;
use crate::publish_checks::_helpers::names::{check_key, not_published, resolved};
use crate::publish_checks::models::CheckedCommit;

/// Run `checks` on `commit`'s staged rows: a failed check fails the commit before it publishes its rows, naming the check with its counts and examples; one listed in `warn` is logged instead.
pub async fn run_checks(
    commit: &CheckedCommit<'_>,
    checks: &TableChecks,
) -> Result<(), LoaderError> {
    let checks = &resolved(checks, commit.columns);
    let (warned, failed): (Vec<_>, Vec<_>) = failures(commit, checks)
        .await?
        .into_iter()
        .partition(|(check, _)| checks.warn.contains(check));
    for (check, detail) in &warned {
        tracing::warn!(table = %format!("{}.{}", commit.table.schema, commit.table.table), check = check_key(*check), %detail, "check failed; publishing anyway, as checks.warn lists it");
    }
    let messages: Vec<String> = failed
        .iter()
        .map(|(check, detail)| format!("checks.{} failed: {detail}", check_key(*check)))
        .collect();
    match messages.is_empty() {
        true => Ok(()),
        false => Err(LoaderError::Invalid(format!(
            "{}; {}",
            messages.join("; "),
            not_published(commit.committed)
        ))),
    }
}
