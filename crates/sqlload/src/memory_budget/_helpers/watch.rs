//! The table the running task loads, for the code loading it to report what it holds and to learn whether to hold back; and the watchdog's readings and stops.

use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use crate::memory_budget::constants::{RUN_STOP_GRACE, RUN_STOP_PERCENT, STOP_PERCENT};
use crate::memory_budget::models::MemoryGuard;
use crate::number_text::main::memory_text::memory_text;
use crate::peak_memory::main::held_bytes::held_bytes;

/// Set once the watchdog stopped the whole run, for the command to exit once it has reported.
pub(crate) static RUN_STOPPED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

tokio::task_local! {
    /// The table this task loads, with its run's guard; unset outside a watched load.
    pub(crate) static TABLE_WATCH: crate::memory_budget::models::WatchedTable;
}

/// In a test build, 92% of the budget while the table `SQLLOAD_TEST_WATCH_TRIP` names holds rows, so a test trips the watchdog's table stop on it, short of the run stop at 95%.
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
    holding.then(|| guard.budget.bytes / 100 * 92)
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

/// Stop the table holding the most large values in memory, unless one stopped already is still ending: say why, and interrupt its statements. A table holding none is never stopped, since stopping it frees nothing the budget counts on. Returns whether a table is stopping, so whether memory may still come down.
pub(crate) fn stop_largest(guard: &MemoryGuard, held: u64) -> bool {
    let tables = guard.tables.lock().unwrap_or_else(PoisonError::into_inner);
    let stopping = tables.iter().any(|table| {
        table
            .tripped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    });
    if stopping {
        return true;
    }
    let Some(largest) = tables
        .iter()
        .filter(|table| table.held.load(Ordering::Relaxed) > 0)
        .max_by_key(|table| table.held.load(Ordering::Relaxed))
    else {
        return false;
    };
    let budget = guard.budget.bytes;
    let percent = u128::from(held) * 100 / u128::from(budget.max(1));
    *largest
        .tripped
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(format!(
        "{} stopped: the load held {} of its {} memory budget ({percent}%); from {STOP_PERCENT}% the table holding the most large values in memory stops, here this one with {}; raise memory_limit, or lower batch_size",
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
    true
}

/// At a reading of `percent` of the budget, with `over` readings since memory reached 95% without coming back under 90%, and `stopping` when a table stopped at 90% is still ending: the count after this reading, and whether the run stops. It stops at 95% when no table is stopping, or one has been for more than [`RUN_STOP_GRACE`] readings without memory falling under 90%.
pub(crate) fn run_stop_due(percent: u128, over: u32, stopping: bool) -> (u32, bool) {
    let past_run_stop = percent >= u128::from(RUN_STOP_PERCENT);
    let over = match percent >= u128::from(STOP_PERCENT) && (past_run_stop || over > 0) {
        true => over.saturating_add(1),
        false => 0,
    };
    (over, past_run_stop && (!stopping || over > RUN_STOP_GRACE))
}

/// Stop the whole run, once: say why, mark every table stopped so none writes or commits again, interrupt their statements (rolling them back), and wake the queue, which fails the tables running and starts no other; the command then reports and exits, so a table busy in its source (a Python function building a list) doesn't hold the run. A MotherDuck bridge ends with the process, rolling back what it had open.
pub(crate) fn stop_run(guard: &MemoryGuard, held: u64, stopping: bool) {
    let mut reason = guard
        .run_stop
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if reason.is_some() {
        return;
    }
    let budget = guard.budget.bytes;
    let percent = u128::from(held) * 100 / u128::from(budget.max(1));
    let why = match stopping {
        true => format!(
            "the table stopped at {STOP_PERCENT}% didn't bring it back under {STOP_PERCENT}%"
        ),
        false => "no table held large values to stop".to_owned(),
    };
    let text = format!(
        "stopped: the run held {} of its {} memory budget ({percent}%), and from {RUN_STOP_PERCENT}% it stops as {why}; raise memory_limit, or lower concurrency",
        memory_text(held),
        memory_text(budget)
    );
    *reason = Some(text.clone());
    drop(reason);
    RUN_STOPPED.store(true, Ordering::Relaxed);
    let tables = guard.tables.lock().unwrap_or_else(PoisonError::into_inner);
    for table in tables.iter() {
        table
            .tripped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| text.clone());
        let interrupts = table
            .interrupts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for interrupt in interrupts.iter() {
            let _ = interrupt.send(());
        }
    }
    drop(tables);
    guard.run_stopped.cancel();
}
