//! One table's load under the watchdog.

use std::sync::{Arc, PoisonError};

use crate::memory_budget::_helpers::watch::TABLE_WATCH;
use crate::memory_budget::models::{MemoryGuard, TableWatch, WatchedTable};

/// `work`, the load of the table named `name`, watched by `guard` (unwatched without one); and why the watchdog stopped it, when it did. A table starting after the run stopped is stopped from the start.
pub async fn watch_table<T>(
    guard: Option<&Arc<MemoryGuard>>,
    name: &str,
    work: impl Future<Output = T>,
) -> (T, Option<String>) {
    let Some(guard) = guard else {
        return (work.await, None);
    };
    let table = Arc::new(TableWatch {
        name: name.to_owned(),
        held: std::sync::atomic::AtomicU64::new(0),
        interrupts: std::sync::Mutex::default(),
        tripped: std::sync::Mutex::default(),
    });
    guard
        .tables
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(table.clone());
    let run_stop = guard
        .run_stop
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    if let Some(reason) = run_stop {
        table
            .tripped
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(reason);
    }
    let watched = WatchedTable {
        guard: guard.clone(),
        table: table.clone(),
    };
    let output = TABLE_WATCH.scope(watched, work).await;
    guard
        .tables
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|other| !Arc::ptr_eq(other, &table));
    let tripped = table
        .tripped
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    (output, tripped)
}
