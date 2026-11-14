//! The progress of the plan running now, which only the line `plan` shows on a terminal reads.

use std::sync::atomic::AtomicUsize;

/// The sources the plan running now has finished checking.
pub(crate) static PLANNED_SOURCES: AtomicUsize = AtomicUsize::new(0);
