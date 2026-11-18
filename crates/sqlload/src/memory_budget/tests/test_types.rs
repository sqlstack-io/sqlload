use crate::memory_budget::models::{BudgetOrigin, LimitOrigin};

pub(crate) struct PlanBudgetTestCase {
    pub(crate) description: &'static str,
    pub(crate) set: Option<(u64, BudgetOrigin)>,
    pub(crate) limit: Option<(u64, LimitOrigin)>,
    pub(crate) overhead: u64,
    pub(crate) duckdb: usize,
    pub(crate) concurrency: usize,
    pub(crate) cores: u64,
    /// The DuckDB share's bytes, `memory_limit`, threads and commit bytes; `None` without one.
    pub(crate) expected_duckdb: Option<(u64, u64, u64, u64)>,
    /// Each table's share and how many load at once.
    pub(crate) expected_tables: (u64, usize),
    /// `plan`'s `memory` line, or why the plan fails.
    pub(crate) expected_line: Result<&'static str, &'static str>,
}

pub(crate) struct CgroupLimitTestCase {
    pub(crate) description: &'static str,
    /// `/proc/self/cgroup` as read.
    pub(crate) groups: &'static str,
    /// Each folder under the cgroup root with its `memory.max`.
    pub(crate) limits: &'static [(&'static str, &'static str)],
    pub(crate) expected_limit: Option<u64>,
}

pub(crate) struct LowerLimitTestCase {
    pub(crate) description: &'static str,
    /// A cgroup's or job object's limit.
    pub(crate) limit: Option<(u64, LimitOrigin)>,
    /// The machine's memory.
    pub(crate) physical: Option<u64>,
    pub(crate) expected_limit: Option<(u64, LimitOrigin)>,
}

/// Watchdog readings, each a percent of the budget and whether a table stopped at 90% is still ending, and the reading at which the run stops.
pub(crate) struct RunStopTestCase {
    pub(crate) description: &'static str,
    pub(crate) readings: Vec<(u128, bool)>,
    pub(crate) expected_stop: Option<usize>,
}

/// A `memory_limit` as written, or as DuckDB's `current_setting('memory_limit')` prints it, and the setting read from it.
pub(crate) struct MemorySettingTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) expected_setting: Result<crate::memory_budget::models::MemorySetting, &'static str>,
}

/// Helper processes a guard watches, the first `dropped` of them ended, and the ids it still counts.
pub(crate) struct WatchedChildTestCase {
    pub(crate) description: &'static str,
    pub(crate) pids: &'static [u32],
    pub(crate) dropped: usize,
    pub(crate) expected_counted: &'static [u32],
}
