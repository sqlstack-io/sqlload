//! A run's memory budget, worked out before its targets connect.

use crate::memory_budget::_helpers::budget_line::budget_line;
use crate::memory_budget::_helpers::limit::memory_limit;
use crate::memory_budget::_helpers::plan_budget::plan_budget;
use crate::memory_budget::models::{BudgetOrigin, BudgetRequest, MemorySetting, RunMemory};
use crate::peak_memory::main::held_bytes::held_bytes;

/// The budget of a run with `set` (`--memory-limit`, else the project's `memory_limit`), `duckdb` DuckDB or MotherDuck targets and `concurrency` tables at once, from the limit the OS sets and what the process holds now; and its `plan` line. Without a limit or a figure for what the process holds, the run has no budget and the line says why; set `off`, it has none, and the line, which `plan` always shows, says so.
pub fn run_memory(
    set: Option<(MemorySetting, BudgetOrigin)>,
    duckdb: usize,
    concurrency: usize,
) -> RunMemory {
    let set = match set {
        Some((MemorySetting::Off, origin)) => {
            let setting = match origin {
                BudgetOrigin::Flag => "--memory-limit",
                BudgetOrigin::Setting | BudgetOrigin::Detected => "memory_limit",
            };
            return RunMemory {
                budget: None,
                line: Some(format!(
                    "off ({setting}): no budget or watchdog, and DuckDB keeps its own memory_limit and threads"
                )),
                asks: true,
                problem: None,
                guard: None,
            };
        }
        Some((MemorySetting::Limit(bytes), origin)) => Some((bytes, origin)),
        None => None,
    };
    let (limit, unreadable) = match memory_limit() {
        Ok(limit) => (limit, None),
        Err(reason) => (None, Some(reason)),
    };
    let overhead = match held_bytes(None) {
        Ok(bytes) => bytes,
        Err(reason) => {
            return RunMemory {
                budget: None,
                line: Some(budget_line(None, (concurrency, Some(&reason)))),
                asks: true,
                problem: None,
                guard: None,
            };
        }
    };
    let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get() as u64);
    let request = BudgetRequest {
        set,
        limit,
        overhead,
        duckdb,
        concurrency,
        cores,
    };
    match plan_budget(&request) {
        Ok(budget) => RunMemory {
            line: Some(budget_line(
                budget.as_ref(),
                (concurrency, unreadable.as_deref()),
            )),
            asks: budget
                .as_ref()
                .is_none_or(|budget| budget.tables_at_once < concurrency.max(1)),
            budget,
            problem: None,
            guard: None,
        },
        Err(problem) => RunMemory {
            budget: None,
            line: None,
            asks: false,
            problem: Some(problem),
            guard: None,
        },
    }
}
