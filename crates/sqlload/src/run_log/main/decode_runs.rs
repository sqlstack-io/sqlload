//! Read run log records back.

use chrono::DateTime;
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
        recorded_at: dated(field("recorded_at")),
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
    let record = serde_json::from_value::<RunRecord>(value.clone())
        .map_err(|error| unreadable(format!("does not parse: {error}")))?;
    match DateTime::parse_from_rfc3339(&record.recorded_at) {
        Ok(_) => Ok(record),
        Err(error) => Err(unreadable(format!(
            "recorded_at {:?} is not an RFC 3339 time; {error}",
            record.recorded_at
        ))),
    }
}

/// `text` when it is an RFC 3339 time, as SQLLoad stamps records; an unreadable record with any other time counts as undated.
fn dated(text: Option<String>) -> Option<String> {
    let text = text?;
    match DateTime::parse_from_rfc3339(&text) {
        Ok(_) => Some(text),
        Err(error) => {
            tracing::debug!(%error, text, "an unreadable run log record's time is not RFC 3339");
            None
        }
    }
}
