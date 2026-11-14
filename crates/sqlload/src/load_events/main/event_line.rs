//! A load event as one line of JSON.

use crate::load_events::models::LoadEvent;

/// `event` as one line of JSON, as `--events jsonl` writes it.
pub fn event_line(event: &LoadEvent) -> String {
    serde_json::to_string(event).unwrap_or_else(|error| {
        serde_json::json!({ "event": "unwritable", "error": error.to_string() }).to_string()
    })
}
