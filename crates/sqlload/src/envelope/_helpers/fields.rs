use serde_json::{Map, Value};

use crate::config::models::{ColumnConfig, EnvelopeField};
use crate::envelope::models::KafkaRecord;
use crate::time_text::main::iso_utc::iso_utc;
use crate::time_text::main::utc_date::utc_date;

/// The JSON value of one column for one record.
pub(crate) fn field_value(
    column: &ColumnConfig,
    record: &KafkaRecord,
    exported_at_ms: i64,
) -> Value {
    match column.field {
        EnvelopeField::Topic => Value::from(record.topic.as_str()),
        EnvelopeField::Partition => Value::from(record.partition),
        EnvelopeField::Offset => Value::from(record.offset),
        EnvelopeField::Key => text(record.key.as_deref()),
        EnvelopeField::Timestamp => record.timestamp_ms.map_or(Value::Null, Value::from),
        EnvelopeField::TimestampType => record.timestamp_type.map_or(Value::Null, Value::from),
        EnvelopeField::Value => payload(column, record.value.as_deref()),
        EnvelopeField::Headers => headers(record),
        EnvelopeField::ExportedAt => Value::from(iso_utc(exported_at_ms)),
        EnvelopeField::Date => Value::from(utc_date(record.timestamp_ms.unwrap_or(exported_at_ms))),
        EnvelopeField::Null => Value::Null,
    }
}

fn text(bytes: Option<&[u8]>) -> Value {
    bytes.map_or(Value::Null, |bytes| {
        Value::from(String::from_utf8_lossy(bytes).into_owned())
    })
}

fn payload(column: &ColumnConfig, bytes: Option<&[u8]>) -> Value {
    let Some(bytes) = bytes else {
        return Value::Null;
    };
    if is_semi_structured(&column.sql_type)
        && let Ok(parsed) = serde_json::from_slice::<Value>(bytes)
    {
        return parsed;
    }
    Value::from(String::from_utf8_lossy(bytes).into_owned())
}

fn headers(record: &KafkaRecord) -> Value {
    let mut object = Map::with_capacity(record.headers.len());
    for header in &record.headers {
        object.insert(header.name.clone(), text(header.value.as_deref()));
    }
    Value::Object(object)
}

/// True for column types that hold parsed JSON rather than text.
fn is_semi_structured(sql_type: &str) -> bool {
    let upper = sql_type.trim().to_ascii_uppercase();
    upper.starts_with("VARIANT") || upper.starts_with("OBJECT") || upper.starts_with("ARRAY")
}

/// The record's value parsed as JSON, if it is JSON.
pub(crate) fn parsed_value(record: &KafkaRecord) -> Option<Value> {
    let bytes = record.value.as_deref()?;
    match serde_json::from_slice::<Value>(bytes) {
        Ok(parsed) => Some(parsed),
        Err(_) => None,
    }
}

/// One member of the parsed value: compact JSON text, or the JSON itself for semi-structured columns; null when absent.
pub(crate) fn member_value(column: &ColumnConfig, parsed: Option<&Value>, member: &str) -> Value {
    match parsed.and_then(|value| value.get(member)) {
        None | Some(Value::Null) => Value::Null,
        Some(found) if is_semi_structured(&column.sql_type) => found.clone(),
        Some(found) => Value::from(found.to_string()),
    }
}
