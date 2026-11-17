//! The watchdog keeping a load under its memory budget.

use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError, Weak};

use crate::memory_budget::constants::{HOLD_PERCENT, STOP_PERCENT, WATCH_INTERVAL};
use crate::memory_budget::models::{MemoryBudget, MemoryGuard};
use crate::number_text::main::memory_text::memory_text;
use crate::peak_memory::main::held_bytes::held_bytes;

/// A guard over `budget`, read by a thread of its own every 10 ms until the guard is dropped: what the process and its MotherDuck bridges hold, against the budget. From 80% of it no table or piece starts; from 90% the table holding the most is stopped, one at a time: its DuckDB statements are interrupted (rolling back) and it fails, naming the fix, while the other tables load on. The process is never stopped.
pub fn start_watchdog(budget: MemoryBudget) -> Arc<MemoryGuard> {
    let guard = Arc::new(MemoryGuard {
        budget,
        tables: std::sync::Mutex::default(),
        children: std::sync::Mutex::default(),
        holding: std::sync::atomic::AtomicBool::new(false),
        released: tokio::sync::Notify::new(),
        peak: std::sync::atomic::AtomicU64::new(0),
    });
    let watched = Arc::downgrade(&guard);
    let started = std::thread::Builder::new()
        .name("sqlload-memory".to_owned())
        .spawn(move || watch(&watched));
    if let Err(error) = started {
        tracing::warn!(%error, "the memory watchdog could not start; the load runs without it");
    }
    guard
}

/// Read the memory every interval until the guard is gone.
fn watch(watched: &Weak<MemoryGuard>) {
    loop {
        std::thread::sleep(WATCH_INTERVAL);
        let Some(guard) = watched.upgrade() else {
            return;
        };
        let held = match process_held(&guard) {
            Ok(held) => held,
            Err(error) => {
                tracing::warn!(%error, "the memory watchdog stopped: it can't read the process's memory");
                return;
            }
        };
        #[cfg(feature = "container-tests")]
        let held = forced_trip(&guard).unwrap_or(held);
        guard.peak.fetch_max(held, Ordering::Relaxed);
        let percent = u128::from(held) * 100 / u128::from(guard.budget.bytes.max(1));
        let holding = percent >= u128::from(HOLD_PERCENT);
        if guard.holding.swap(holding, Ordering::Relaxed) && !holding {
            guard.released.notify_waiters();
        }
        if percent >= u128::from(STOP_PERCENT) {
            stop_largest(&guard, held);
        }
    }
}

/// In a test build, 95% of the budget while the table `SQLLOAD_TEST_WATCH_TRIP` names holds rows, so a test trips the watchdog on it.
#[cfg(feature = "container-tests")]
fn forced_trip(guard: &MemoryGuard) -> Option<u64> {
    let named = std::env::var("SQLLOAD_TEST_WATCH_TRIP").ok()?;
    let holding = guard
        .tables
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|table| table.name.ends_with(&named) && table.held.load(Ordering::Relaxed) > 0);
    holding.then(|| guard.budget.bytes / 100 * 95)
}

/// What the process and its bridges hold now.
fn process_held(guard: &MemoryGuard) -> Result<u64, String> {
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
fn stop_largest(guard: &MemoryGuard, held: u64) {
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
        "{} stopped: the load held {} of its {} memory budget ({percent}%, past the {STOP_PERCENT}% at which the table holding the most stops), this table {}; raise memory_limit, or lower batch_size",
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
