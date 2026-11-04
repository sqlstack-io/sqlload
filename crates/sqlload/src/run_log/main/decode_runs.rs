//! Read run log records back.

use crate::run_log::models::RunRecord;

/// The records in `texts` (one JSON object each), oldest first; a record that does not parse is an error naming it.
pub fn decode_runs(texts: &[String]) -> Result<Vec<RunRecord>, String> {
    let mut records: Vec<RunRecord> = texts
        .iter()
        .map(|text| {
            serde_json::from_str::<RunRecord>(text)
                .map_err(|error| format!("a run log record does not parse ({error}): {text}"))
        })
        .collect::<Result<_, String>>()?;
    records.sort_by(|left, right| left.recorded_at.cmp(&right.recorded_at));
    Ok(records)
}
