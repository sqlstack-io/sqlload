//! The budget one fetch of a table takes of its share of the run's memory budget, and how `plan` shows it.

use crate::memory_budget::constants::TABLE_SHARE_PARTS;
use crate::number_text::main::memory_text::memory_text;
use crate::odbc_source::constants::{DEFAULT_BATCH_MEMORY, NAMED_LONG_COLUMNS};
use crate::odbc_source::models::{BudgetReason, FetchShape, FetchSize, MemoryBudget};

/// The budget of one fetch: `set` (the source's `max_batch_memory`) as it is; else 512 MiB, lowered to a quarter of the table's `share` of the run's memory budget when that is less (never raised); 512 MiB without a budget.
pub(crate) fn fetch_budget(set: Option<usize>, share: Option<u64>) -> MemoryBudget {
    let part = share.map(|share| {
        usize::try_from(share / TABLE_SHARE_PARTS)
            .unwrap_or(usize::MAX)
            .max(1)
    });
    let (bytes, reason) = match (set, part) {
        (Some(bytes), _) => (bytes.max(1), BudgetReason::Set),
        (None, Some(part)) if part < DEFAULT_BATCH_MEMORY => (part, BudgetReason::Capped),
        (None, _) => (DEFAULT_BATCH_MEMORY, BudgetReason::Default),
    };
    MemoryBudget {
        bytes,
        reason,
        share,
    }
}

/// `plan`'s `buffer` line: the fetch buffer (the budget) per table, and why.
pub(crate) fn budget_note(budget: &MemoryBudget) -> String {
    let bytes = memory_text(budget.bytes as u64);
    match (budget.reason, budget.share) {
        (BudgetReason::Set, _) => format!("{bytes} fetch buffer per table (max_batch_memory)"),
        (BudgetReason::Capped, Some(share)) => format!(
            "{bytes} fetch buffer per table (capped: a quarter of each table's {} share of the memory budget)",
            memory_text(share)
        ),
        (_, Some(share)) => format!(
            "{bytes} fetch buffer per table (default; each table's share of the memory budget is {})",
            memory_text(share)
        ),
        (_, None) => {
            format!("{bytes} fetch buffer per table (default; the run has no memory budget)")
        }
    }
}

/// `plan`'s `fetch` line for a table read as `shape` in fetches of `size`: in bulk, the bytes and rows of one fetch; value by value, the budget and rows that end a fetch, and the long columns, the first few named.
pub(crate) fn memory_line(shape: &FetchShape, size: &FetchSize) -> String {
    let rows = thousands(size.rows);
    if shape.long_columns.is_empty() {
        return format!("~{} per fetch, {rows} rows", memory_text(size.bytes as u64));
    }
    let count = shape.long_columns.len();
    let mut names = shape.long_columns[..count.min(NAMED_LONG_COLUMNS)].join(", ");
    if count > NAMED_LONG_COLUMNS {
        names.push_str(", …");
    }
    let columns = match count {
        1 => format!("1 long column ({names})"),
        _ => format!("{count} long columns ({names})"),
    };
    format!(
        "up to {} or {rows} rows per fetch; {columns}, so all {} columns are read value by value",
        memory_text(size.bytes as u64),
        shape.columns
    )
}

/// `count` with thousands separators, as `65,536`.
fn thousands(count: usize) -> String {
    let digits = count.to_string();
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}
