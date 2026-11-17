//! How many of a table's cursor windows its read takes at once.

use crate::loader::models::{CommitRule, TablePlan};

/// The most cursor windows `table`'s read takes at once under `batch_concurrency`, each over its own Postgres connection in one shared snapshot and still committed in order: the setting, but no more than the table has windows; 1 for any other source or commit rule, which reads its windows one at a time.
pub fn window_concurrency(table: &TablePlan) -> usize {
    let spec = &table.spec;
    let postgres = spec.python.is_none()
        && spec.files.is_none()
        && spec.sheet.is_none()
        && spec.warehouse_source.is_none();
    match &table.commits {
        CommitRule::Windows(windows) if postgres => spec
            .batch_concurrency
            .min(usize::try_from(windows.count).unwrap_or(usize::MAX))
            .max(1),
        _ => 1,
    }
}
