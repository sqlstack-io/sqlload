//! Read run log records back.

use serde_json::Value;

use crate::run_log::constants::RECORD_FORMAT_VERSION;
use crate::run_log::models::{DecodedRuns, RunRecord, UnreadableRecord};

/// The records in `texts` (one JSON object each), ordered by time then run id, and the ones that cannot be used: those that do not parse, and those a newer SQLLoad wrote.
pub fn decode_runs(texts: &[String]) -> DecodedRuns {
    let mut decoded = DecodedRuns::default();
    for text in texts {
        match serde_json::from_str::<Value>(text) {
            Ok(value) => match decode_one(value) {
                Ok(record) => decoded.records.push(record),
                Err(unreadable) => decoded.unreadable.push(unreadable),
            },
            Err(error) => decoded.unreadable.push(UnreadableRecord {
                run_id: None,
                recorded_at: None,
                reason: format!("does not parse: {error}"),
            }),
        }
    }
    decoded.records.sort_by(|left, right| {
        (&left.recorded_at, &left.run_id).cmp(&(&right.recorded_at, &right.run_id))
    });
    decoded
}

/// One record read as JSON, or why it cannot be used, naming its run id and time.
fn decode_one(value: Value) -> Result<RunRecord, UnreadableRecord> {
    let field = |name: &str| value.get(name).and_then(Value::as_str).map(str::to_owned);
    let unreadable = |reason: String| UnreadableRecord {
        run_id: field("run_id"),
        recorded_at: field("recorded_at"),
        reason,
    };
    let version = value
        .get("format_version")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    if version > u64::from(RECORD_FORMAT_VERSION) {
        return Err(unreadable(format!(
            "written by a newer sqlload (format_version {version}, this one reads {RECORD_FORMAT_VERSION}); upgrade sqlload"
        )));
    }
    serde_json::from_value::<RunRecord>(value.clone())
        .map_err(|error| unreadable(format!("does not parse: {error}")))
}
