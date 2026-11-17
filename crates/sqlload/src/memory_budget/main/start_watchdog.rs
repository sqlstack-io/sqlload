//! The watchdog keeping a load under its memory budget.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use crate::memory_budget::_helpers::watch::{process_held, stop_largest};
use crate::memory_budget::constants::{HOLD_PERCENT, STOP_PERCENT, WATCH_INTERVAL};
use crate::memory_budget::models::{MemoryBudget, MemoryGuard};

/// A guard over `budget`, read by a thread of its own every 10 ms until the guard is dropped: what the process and its MotherDuck bridges hold, against the budget. From 80% of it no table starts and pieces are cut smaller; from 90% the table holding the most large values is stopped, one at a time: its DuckDB statements are interrupted (rolling back) and it fails, naming the fix, while the other tables load on. The process is never stopped.
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
        let held = crate::memory_budget::_helpers::watch::forced_trip(&guard).unwrap_or(held);
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
