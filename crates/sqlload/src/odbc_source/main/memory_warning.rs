//! Whether an ODBC source's fetches outgrow their part of the tables' shares of the memory budget.

use crate::memory_budget::constants::TABLE_SHARE_PARTS;
use crate::number_text::main::memory_text::memory_text;

/// A warning when any of `fetches` (each table's planned bytes per fetch, as the driver describes its columns, and its source) holds more than a quarter of each table's `share` of the run's memory budget, the part a fetch gets, naming the largest, the sources and the fix; `None` otherwise, or without a budget.
pub fn memory_warning(fetches: &[(usize, &str)], share: Option<u64>) -> Option<String> {
    let part = share? / TABLE_SHARE_PARTS;
    let mut over: Vec<(usize, &str)> = fetches
        .iter()
        .copied()
        .filter(|(bytes, _)| *bytes as u64 > part)
        .collect();
    let largest = over.iter().map(|(bytes, _)| *bytes).max()?;
    over.sort_by_key(|(_, source)| *source);
    over.dedup_by_key(|(_, source)| *source);
    let sources: Vec<&str> = over.iter().map(|(_, source)| *source).collect();
    Some(format!(
        "an ODBC fetch may hold {}, more than the {} a fetch gets of each table's {} share of the memory budget; lower max_batch_memory on {} {}, or raise memory_limit",
        memory_text(largest as u64),
        memory_text(part),
        memory_text(share?),
        match sources.len() {
            1 => "source",
            _ => "sources",
        },
        sources.join(", ")
    ))
}
