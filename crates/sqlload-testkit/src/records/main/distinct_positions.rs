//! Count distinct record positions.

use std::collections::BTreeSet;

use serde_json::Value;

/// Distinct (topic, partition, offset) triples among exported rows with the default envelope.
pub fn distinct_positions(rows: &[Value]) -> usize {
    let positions: BTreeSet<(String, i64, i64)> = rows.iter().map(position).collect();
    positions.len()
}

fn position(row: &Value) -> (String, i64, i64) {
    (
        row["TOPIC"].as_str().unwrap_or_default().to_owned(),
        row["KAFKA_PARTITION"].as_i64().unwrap_or(-1),
        row["KAFKA_OFFSET"].as_i64().unwrap_or(-1),
    )
}
