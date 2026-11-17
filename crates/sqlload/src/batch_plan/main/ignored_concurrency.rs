//! Why `batch_concurrency` has no effect.

use crate::batch_plan::main::window_concurrency::window_concurrency;
use crate::loader::models::{CommitRule, TablePlan};

/// Why the table's `batch_concurrency` has no effect, if it is above 1 and has none: only a Postgres source reads windows at once, and only a table committing `batch_size` windows has windows to read; a run with a single window reads it alone without a warning.
pub fn ignored_concurrency(plan: &TablePlan) -> Option<String> {
    if plan
        .spec
        .batch_concurrency
        .is_none_or(|limit| limit.get() <= 1)
        || window_concurrency(plan) > 1
    {
        return None;
    }
    let reason = match &plan.commits {
        CommitRule::Windows(windows) if windows.count <= 1 => return None,
        CommitRule::Windows(_) => "only a Postgres source reads windows at once",
        _ => "the table commits no batch_size windows",
    };
    Some(format!("batch_concurrency is ignored: {reason}"))
}
