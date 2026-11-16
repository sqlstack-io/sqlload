//! A merge table's `deletes`, with the limit of its key scans.

use crate::loader::models::DeletesSpec;
use crate::project::models::{Deletes, TableChecks};
use crate::row_deletes::constants::DEFAULT_MAX_DROP;

/// The table's `deletes` with its key scans' limit, and its `checks` without `max_row_drop`, which under `deletes` limits what a scan marks rather than a commit (`None` once they set no other check).
pub fn deletes_spec(
    deletes: Option<Deletes>,
    checks: Option<TableChecks>,
) -> (Option<DeletesSpec>, Option<TableChecks>) {
    let Some(apply) = deletes else {
        return (None, checks);
    };
    let max_drop = checks
        .as_ref()
        .and_then(|checks| checks.max_row_drop)
        .unwrap_or(DEFAULT_MAX_DROP);
    let checks = checks
        .map(|checks| TableChecks {
            max_row_drop: None,
            ..checks
        })
        .filter(|checks| {
            checks.unique.is_some() || !checks.not_null.is_empty() || checks.min_rows.is_some()
        });
    let spec = DeletesSpec {
        apply,
        max_drop,
        scan: false,
    };
    (Some(spec), checks)
}
