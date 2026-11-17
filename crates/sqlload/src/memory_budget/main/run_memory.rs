//! A run's memory budget, worked out before its targets connect.

use crate::memory_budget::_helpers::budget_line::budget_line;
use crate::memory_budget::_helpers::limit::memory_limit;
use crate::memory_budget::_helpers::plan_budget::plan_budget;
use crate::memory_budget::models::{BudgetOrigin, BudgetRequest, RunMemory};
use crate::peak_memory::main::held_bytes::held_bytes;

/// The budget of a run with `set` (`--memory-limit`, else the project's `memory_limit`), `duckdb` DuckDB or MotherDuck targets and `concurrency` tables at once, from the limit the OS sets and what the process holds now; and its `plan` line. Without a limit or a figure for what the process holds, the run has no budget and the line says why.
pub fn run_memory(
    set: Option<(u64, BudgetOrigin)>,
    duckdb: usize,
    concurrency: usize,
) -> RunMemory {
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
            budget,
            problem: None,
            guard: None,
        },
        Err(problem) => RunMemory {
            budget: None,
            line: None,
            problem: Some(problem),
            guard: None,
        },
    }
}
