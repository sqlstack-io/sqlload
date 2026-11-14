//! How many sources the plan in progress has finished, for the line `plan` shows while it works.

use crate::loader::_helpers::progress::PLANNED_SOURCES;

/// The sources the plan running now has finished checking.
pub fn planned_sources() -> usize {
    PLANNED_SOURCES.load(std::sync::atomic::Ordering::Relaxed)
}
