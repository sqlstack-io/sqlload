//! The run budget that gives DuckDB a `memory_limit`.

use crate::memory_budget::_helpers::plan_budget::setting_text;
use crate::memory_budget::constants::{DUCKDB_FOOTPRINT, DUCKDB_SHARE_PERCENT, MIN_TABLE_SHARE};
use crate::memory_budget::main::table_memory::table_memory;

/// The fix for a commit DuckDB can't hold within its `memory_limit`: `raise memory_limit to at least 2.6GiB`, the budget whose DuckDB share gives each DuckDB target `duckdb_limit`, past what the running load's process held before loading.
pub fn needed_budget(duckdb_limit: u64) -> String {
    let (overhead, targets) = table_memory().map_or((0, 1), |watched| {
        let budget = watched.guard.budget;
        (budget.overhead, budget.duckdb_targets.max(1) as u64)
    });
    let shares =
        u128::from(duckdb_limit) * u128::from(DUCKDB_FOOTPRINT) * u128::from(targets) * 100
            / u128::from(DUCKDB_SHARE_PERCENT);
    let needed = u64::try_from(shares)
        .unwrap_or(u64::MAX)
        .saturating_add(overhead)
        .max(overhead.saturating_add(MIN_TABLE_SHARE));
    format!("raise memory_limit to at least {}", setting_text(needed))
}
