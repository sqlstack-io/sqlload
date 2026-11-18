//! `plan`'s `memory` line: the run's budget, where it came from, and how it is shared.

use crate::memory_budget::constants::{LIMIT_SHARE_PERCENT, MACHINE_SHARE_PERCENT};
use crate::memory_budget::models::{BudgetOrigin, LimitOrigin, MemoryBudget};
use crate::number_text::main::memory_text::memory_text;

/// The budget, where it came from, what the process held before loading, DuckDB's share and settings, and each table's share with how many load at once (and that `concurrency` was lowered to fit); without a budget, why, naming the setting. `unreadable` is why the OS's limit couldn't be read.
pub(crate) fn budget_line(
    budget: Option<&MemoryBudget>,
    (concurrency, unreadable): (usize, Option<&str>),
) -> String {
    let Some(budget) = budget else {
        return match unreadable {
            Some(reason) => format!(
                "no budget: this machine's memory can't be read ({reason}); set memory_limit"
            ),
            None => "no budget: this OS doesn't give its memory; set memory_limit".to_owned(),
        };
    };
    let origin = match (budget.origin, budget.limit) {
        (BudgetOrigin::Setting, _) => "memory_limit".to_owned(),
        (BudgetOrigin::Flag, _) => "--memory-limit".to_owned(),
        (BudgetOrigin::Detected, Some(limit)) => match limit.origin {
            LimitOrigin::Container => format!(
                "{LIMIT_SHARE_PERCENT}% of the {} container limit",
                memory_text(limit.bytes)
            ),
            LimitOrigin::JobObject => format!(
                "{LIMIT_SHARE_PERCENT}% of the {} job object limit",
                memory_text(limit.bytes)
            ),
            LimitOrigin::Machine => format!(
                "{MACHINE_SHARE_PERCENT}% of {} of memory",
                memory_text(limit.bytes)
            ),
        },
        (BudgetOrigin::Detected, None) => "detected".to_owned(),
    };
    let mut parts = vec![
        format!("{} budget ({origin})", memory_text(budget.bytes)),
        format!("{} held before loading", memory_text(budget.overhead)),
    ];
    if let Some(duckdb) = &budget.duckdb {
        let each = match budget.duckdb_targets {
            1 => String::new(),
            targets => format!(" each of {targets}"),
        };
        parts.push(format!(
            "DuckDB {}{each} (memory_limit {}, {} threads)",
            memory_text(duckdb.bytes),
            memory_text(duckdb.memory_limit),
            duckdb.threads
        ));
    }
    let at_once = match budget.tables_at_once < concurrency.max(1) {
        true => format!(
            "{} at once (concurrency {concurrency} doesn't fit)",
            budget.tables_at_once
        ),
        false => format!("{} at once", budget.tables_at_once),
    };
    parts.push(format!(
        "{} per table, {at_once}",
        memory_text(budget.table_share)
    ));
    parts.join("; ")
}
