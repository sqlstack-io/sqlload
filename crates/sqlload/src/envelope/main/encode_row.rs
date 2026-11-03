//! Encode one record as one JSON row.

use serde_json::{Map, Value};

use crate::config::models::ColumnConfig;
use crate::envelope::_helpers::fields::{field_value, member_value, parsed_value};
use crate::envelope::models::{KafkaRecord, RowContext};

/// The record as one JSON object keyed by column name, without a trailing newline; `unwrap` columns take a member of the JSON value.
pub fn encode_row(
    columns: &[ColumnConfig],
    record: &KafkaRecord,
    context: &RowContext<'_>,
) -> String {
    let parsed = match context.unwrap.is_empty() {
        true => None,
        false => parsed_value(record),
    };
    let mut row = Map::with_capacity(columns.len());
    for column in columns {
        let value = match context.unwrap.get(&column.name) {
            Some(member) => member_value(column, parsed.as_ref(), member),
            None => field_value(column, record, context),
        };
        row.insert(column.name.clone(), value);
    }
    Value::Object(row).to_string()
}
