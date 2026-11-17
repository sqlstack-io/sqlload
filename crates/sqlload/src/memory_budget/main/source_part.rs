//! The part of each table's share a source's own buffers get, for readers outside the table's task.

use std::sync::atomic::Ordering;

use crate::memory_budget::_helpers::watch::SOURCE_PART;

/// A quarter of each table's share of the running load's memory budget, the most a source holds ahead of the batches it made, such as the pages an `sl.http` job requests ahead; `None` while no load with a budget runs.
pub fn source_part() -> Option<u64> {
    Some(SOURCE_PART.load(Ordering::Relaxed)).filter(|part| *part > 0)
}
