use crate::config::models::{ColumnConfig, EnvelopeField};
use crate::envelope::models::{KafkaRecord, RecordHeader};

/// One column of every field, with the value column typed `value_type`.
pub(crate) fn all_columns(value_type: &str) -> Vec<ColumnConfig> {
    [
        ("T", EnvelopeField::Topic, "VARCHAR"),
        ("P", EnvelopeField::Partition, "NUMBER"),
        ("O", EnvelopeField::Offset, "NUMBER"),
        ("K", EnvelopeField::Key, "VARCHAR"),
        ("TS", EnvelopeField::Timestamp, "NUMBER"),
        ("TT", EnvelopeField::TimestampType, "NUMBER"),
        ("V", EnvelopeField::Value, value_type),
        ("H", EnvelopeField::Headers, "VARIANT"),
        ("EXPORTED_AT", EnvelopeField::ExportedAt, "TIMESTAMP_TZ"),
        ("DATE", EnvelopeField::Date, "DATE"),
        ("X", EnvelopeField::Null, "VARCHAR"),
    ]
    .into_iter()
    .map(|(name, field, sql_type)| ColumnConfig {
        name: name.to_owned(),
        field,
        sql_type: sql_type.to_owned(),
    })
    .collect()
}

/// A record at offset 42 of partition 3, created at 2024-02-29T12:34:56Z.
pub(crate) fn record(key: Option<&[u8]>, value: Option<&[u8]>) -> KafkaRecord {
    KafkaRecord {
        topic: "shop.orders".to_owned(),
        partition: 3,
        offset: 42,
        key: key.map(<[u8]>::to_vec),
        timestamp_ms: Some(1_709_210_096_000),
        timestamp_type: Some(0),
        value: value.map(<[u8]>::to_vec),
        headers: vec![RecordHeader {
            name: "source".to_owned(),
            value: Some(b"web".to_vec()),
        }],
    }
}
