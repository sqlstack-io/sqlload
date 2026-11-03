use crate::config::models::{ColumnConfig, EnvelopeField};
use crate::file_format::models::ParquetKind;

/// How a column is stored in Parquet: Kafka positions as integers, `exported_at` and `date` typed, everything else text.
pub(crate) fn parquet_kind(column: &ColumnConfig) -> ParquetKind {
    match column.field {
        EnvelopeField::Partition => ParquetKind::Int32,
        EnvelopeField::Offset | EnvelopeField::Timestamp | EnvelopeField::TimestampType => {
            ParquetKind::Int64
        }
        EnvelopeField::ExportedAt => ParquetKind::Timestamp,
        EnvelopeField::Date => ParquetKind::Date,
        EnvelopeField::Topic
        | EnvelopeField::Key
        | EnvelopeField::Value
        | EnvelopeField::Headers
        | EnvelopeField::Null => ParquetKind::Text,
    }
}

/// True for column types that hold parsed JSON rather than text.
pub(crate) fn is_semi_structured(sql_type: &str) -> bool {
    let upper = sql_type.trim().to_ascii_uppercase();
    upper.starts_with("VARIANT") || upper.starts_with("OBJECT") || upper.starts_with("ARRAY")
}

/// `TIMESTAMP_TZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ` (also `TIMESTAMP` and `DATETIME`), or None.
pub(crate) fn timestamp_family(sql_type: &str) -> Option<&'static str> {
    let upper = sql_type.trim().to_ascii_uppercase();
    match upper.split(['(', ' ']).next().unwrap_or_default() {
        "TIMESTAMP_TZ" => Some("TIMESTAMP_TZ"),
        "TIMESTAMP_LTZ" => Some("TIMESTAMP_LTZ"),
        "TIMESTAMP_NTZ" | "TIMESTAMP" | "DATETIME" => Some("TIMESTAMP_NTZ"),
        _ => None,
    }
}
