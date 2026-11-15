//! Whether an ODBC source's fetches, loading at once, near the machine's memory.

use crate::odbc_source::_helpers::memory::memory_text;
use crate::odbc_source::constants::FETCH_SHARE_OF_LIMIT;

/// A warning when the fetch buffers of the `concurrency` largest of `fetches` (each table's planned bytes per fetch, its source, and the machine's memory limit it was planned under), which may load at once, together pass half the limit, the share the budget leaves them, naming the fix and that it counts only those buffers; `None` otherwise, or without a limit.
pub fn memory_warning(
    fetches: &[(usize, &str, Option<u64>)],
    concurrency: usize,
) -> Option<String> {
    let limit = fetches.iter().find_map(|(_, _, limit)| *limit)?;
    let concurrency = concurrency.max(1);
    let mut largest: Vec<(usize, &str)> = fetches
        .iter()
        .map(|(bytes, source, _)| (*bytes, *source))
        .collect();
    largest.sort_by_key(|fetch| std::cmp::Reverse(fetch.0));
    largest.truncate(concurrency);
    let total = largest.iter().fold(0u64, |total, (bytes, _)| {
        total.saturating_add(*bytes as u64)
    });
    if total <= limit / FETCH_SHARE_OF_LIMIT as u64 {
        return None;
    }
    let mut sources: Vec<&str> = largest.iter().map(|(_, source)| *source).collect();
    sources.sort_unstable();
    sources.dedup();
    let tables = match largest.len() {
        1 => "1 table".to_owned(),
        count => format!("{count} tables loading at once"),
    };
    Some(format!(
        "the ODBC fetch buffers of {tables} may hold {} together, more than half of this machine's {} memory limit; this counts only those buffers, not the memory the batches, the destination and the commits take besides; lower max_batch_memory on {} {}, or lower concurrency (now {concurrency})",
        memory_text(total),
        memory_text(limit),
        match sources.len() {
            1 => "source",
            _ => "sources",
        },
        sources.join(", ")
    ))
}
