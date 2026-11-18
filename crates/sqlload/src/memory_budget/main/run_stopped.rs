//! Whether the watchdog stopped the whole run.

use std::sync::atomic::Ordering;

use crate::memory_budget::_helpers::watch::RUN_STOPPED;

/// Whether the memory watchdog stopped this process's run at 95% of its budget: the command reports what it has, then exits at once instead of waiting for tables still busy.
pub fn run_stopped() -> bool {
    RUN_STOPPED.load(Ordering::Relaxed)
}
