//! Encode one record as one JSON row.

use serde_json::{Map, Value};

use crate::config::models::ColumnConfig;
use crate::envelope::_helpers::fields::field_value;
use crate::envelope::models::KafkaRecord;

/// The record as one JSON object keyed by column name, without a trailing newline.
pub fn encode_row(columns: &[ColumnConfig], record: &KafkaRecord, exported_at_ms: i64) -> String {
    let mut row = Map::with_capacity(columns.len());
    for column in columns {
        row.insert(
            column.name.clone(),
            field_value(column, record, exported_at_ms),
        );
    }
    Value::Object(row).to_string()
}
