//! Records for resets and reconciliations.

use crate::run_log::constants::RECORD_FORMAT_VERSION;
use crate::run_log::models::{RecordTable, RunRecord, StateEvent};

/// The record of `event` on `table`, with a fresh id and no rows.
pub fn event_record(table: &RecordTable, event: StateEvent) -> RunRecord {
    RunRecord {
        format_version: RECORD_FORMAT_VERSION,
        run_id: uuid::Uuid::new_v4().to_string(),
        event: event.event,
        source: table.source.clone(),
        source_table: table.source_table.clone(),
        table_name: table.table_name.clone(),
        cursor_column: table.cursor_column.clone(),
        cursor_before: event.cursor_before,
        cursor_after: event.cursor_after,
        state: event.state,
        row_count: 0,
        files: Vec::new(),
        note: Some(event.note),
        recorded_at: event.recorded_at,
        batch: None,
    }
}
