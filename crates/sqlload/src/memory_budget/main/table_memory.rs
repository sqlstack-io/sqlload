//! The watched table the running task loads.

use crate::memory_budget::_helpers::watch::TABLE_WATCH;
use crate::memory_budget::models::WatchedTable;

/// The table the running task loads, with its run's guard; `None` outside a watched load.
pub fn table_memory() -> Option<WatchedTable> {
    TABLE_WATCH.try_with(Clone::clone).ok()
}
