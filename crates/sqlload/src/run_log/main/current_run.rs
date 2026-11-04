//! The table's current logged state.

use crate::run_log::models::{RunEvent, RunRecord};

/// The record holding the table's state: the newest run, reset or reconciliation (backfills never move it); `records` are oldest first.
pub fn current_run(records: &[RunRecord]) -> Option<&RunRecord> {
    records
        .iter()
        .rev()
        .find(|record| record.event != RunEvent::Backfill)
}
