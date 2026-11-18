//! Count a helper process's memory with the load's.

use std::sync::Arc;

use crate::memory_budget::models::{MemoryGuard, WatchedChild};

/// Have `guard` count process `pid`'s memory with SQLLoad's until the returned handle is dropped, so a process that has ended (and whose id may be reused) stops counting.
pub fn watch_child(guard: &Arc<MemoryGuard>, pid: u32) -> WatchedChild {
    guard
        .children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(pid);
    WatchedChild {
        guard: guard.clone(),
        pid,
    }
}
