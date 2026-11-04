//! Describe a logged table's state for people.

use crate::run_log::models::{RunEvent, RunRecord};

/// `cursor <value>, state <json> (<event> <run id> at <time>)`, or that no run is logged yet.
pub fn describe_state(current: Option<&RunRecord>) -> String {
    let Some(record) = current else {
        return "no runs logged yet; the first run reads as a stateless one would".to_owned();
    };
    let mut parts: Vec<String> = Vec::new();
    if let Some(column) = &record.cursor_column {
        parts.push(format!(
            "{column} {}",
            record.cursor_after.as_deref().unwrap_or("not set")
        ));
    }
    parts.push(format!(
        "state {}",
        record.state.as_deref().unwrap_or("not set")
    ));
    let origin = match record.event {
        RunEvent::Reset => "reset",
        RunEvent::Reconcile => "reconciled",
        RunEvent::Run | RunEvent::Backfill => "run",
    };
    format!(
        "{} ({origin} {} at {})",
        parts.join(", "),
        record.run_id,
        record.recorded_at
    )
}
