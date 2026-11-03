//! Encode one record as one JSON row.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::config::models::ColumnConfig;
use crate::envelope::_helpers::fields::{field_value, member_value, parsed_value};
use crate::envelope::models::KafkaRecord;

/// The record as one JSON object keyed by column name, without a trailing newline; `unwrap` columns take a member of the JSON value.
pub fn encode_row(
    columns: &[ColumnConfig],
    record: &KafkaRecord,
    exported_at_ms: i64,
    unwrap: &BTreeMap<String, String>,
) -> String {
    let parsed = match unwrap.is_empty() {
        true => None,
        false => parsed_value(record),
    };
    let mut row = Map::with_capacity(columns.len());
    for column in columns {
        let value = match unwrap.get(&column.name) {
            Some(member) => member_value(column, parsed.as_ref(), member),
            None => field_value(column, record, exported_at_ms),
        };
        row.insert(column.name.clone(), value);
    }
    Value::Object(row).to_string()
}
