//! Complete a run record once the run's rows are written.

use crate::column_types::models::ColumnType;
use crate::run_log::main::later_cursor::later_cursor;
use crate::run_log::models::{RunOutcome, RunRecord};

/// `record` with the run's rows, files and the data's position after it: the larger of the data's maximum before (`before`, unless the run replaced every row) and the loaded rows' maximum.
pub fn finished_record(
    record: &RunRecord,
    cursor_type: Option<&ColumnType>,
    before: Option<String>,
    outcome: RunOutcome,
) -> RunRecord {
    let cursor_after = match cursor_type {
        Some(column_type) => later_cursor(before, outcome.loaded_maximum, column_type),
        None => None,
    };
    RunRecord {
        run_id: outcome.run_id,
        recorded_at: outcome.recorded_at,
        row_count: outcome.rows,
        files: outcome.files,
        cursor_after,
        ..record.clone()
    }
}
