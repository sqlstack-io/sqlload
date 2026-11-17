//! The table the running task loads, for the code loading it to report what it holds and to learn whether to hold back; and the watchdog's readings and stops.

use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use crate::memory_budget::constants::STOP_PERCENT;
use crate::memory_budget::models::MemoryGuard;
use crate::number_text::main::memory_text::memory_text;
use crate::peak_memory::main::held_bytes::held_bytes;

/// A quarter of each table's share of the running load's budget (see [`source_part`](crate::memory_budget::main::source_part::source_part)); 0 while no load with a budget runs.
pub(crate) static SOURCE_PART: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

tokio::task_local! {
    /// The table this task loads, with its run's guard; unset outside a watched load.
    pub(crate) static TABLE_WATCH: crate::memory_budget::models::WatchedTable;
}

/// In a test build, 95% of the budget while the table `SQLLOAD_TEST_WATCH_TRIP` names holds rows, so a test trips the watchdog on it.
#[cfg(feature = "container-tests")]
pub(crate) fn forced_trip(guard: &MemoryGuard) -> Option<u64> {
    let Ok(named) = std::env::var("SQLLOAD_TEST_WATCH_TRIP") else {
        return None;
    };
    let holding = guard
        .tables
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|table| table.name.ends_with(&named) && table.held.load(Ordering::Relaxed) > 0);
    holding.then(|| guard.budget.bytes / 100 * 95)
}

/// What the process and its bridges hold now.
pub(crate) fn process_held(guard: &MemoryGuard) -> Result<u64, String> {
    let children = guard
        .children
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let mut held = held_bytes(None)?;
    for child in children {
        held = held.saturating_add(held_bytes(Some(child)).unwrap_or_default());
    }
    Ok(held)
}

/// Stop the table holding the most, unless one stopped already is still ending: say why, and interrupt its statements.
pub(crate) fn stop_largest(guard: &MemoryGuard, held: u64) {
    let tables = guard.tables.lock().unwrap_or_else(PoisonError::into_inner);
    let stopping = tables.iter().any(|table| {
        table
            .tripped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    });
    if stopping {
        return;
    }
    let Some(largest) = tables
        .iter()
        .max_by_key(|table| table.held.load(Ordering::Relaxed))
    else {
        return;
    };
    let budget = guard.budget.bytes;
    let percent = u128::from(held) * 100 / u128::from(budget.max(1));
    *largest
        .tripped
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(format!(
        "{} stopped: the load held {} of its {} memory budget ({percent}%); from {STOP_PERCENT}% the table holding the most rows in memory stops, here this one with {}; raise memory_limit, or lower batch_size",
        largest.name,
        memory_text(held),
        memory_text(budget),
        memory_text(largest.held.load(Ordering::Relaxed))
    ));
    for interrupt in largest
        .interrupts
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
    {
        let _ = interrupt.send(());
    }
}
