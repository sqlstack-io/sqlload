//! The table the running task loads, for the code loading it to report what it holds and to learn whether to hold back.

tokio::task_local! {
    /// The table this task loads, with its run's guard; unset outside a watched load.
    pub(crate) static TABLE_WATCH: crate::memory_budget::models::WatchedTable;
}
