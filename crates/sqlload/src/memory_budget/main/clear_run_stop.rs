//! Forget an earlier run's stop, for a process running several commands.

use std::sync::atomic::Ordering;

use crate::memory_budget::_helpers::watch::RUN_STOPPED;

/// Clear the mark the memory watchdog leaves when it stops a run, so a later command in the same process (the Python package runs several) isn't taken for stopped.
pub fn clear_run_stop() {
    RUN_STOPPED.store(false, Ordering::Relaxed);
}
