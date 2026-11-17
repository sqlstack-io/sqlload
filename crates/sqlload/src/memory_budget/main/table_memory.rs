//! The watched table the running task loads.

use crate::memory_budget::_helpers::watch::TABLE_WATCH;
use crate::memory_budget::models::WatchedTable;

/// The table the running task loads, with its run's guard; `None` outside a watched load.
pub fn table_memory() -> Option<WatchedTable> {
    let Ok(watched) = TABLE_WATCH.try_with(Clone::clone) else {
        return None;
    };
    Some(watched)
}
