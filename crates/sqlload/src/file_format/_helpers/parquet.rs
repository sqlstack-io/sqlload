use std::sync::Arc;

use arrow_array::builder::{
    Date32Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::{Map, Value};

use crate::config::models::ColumnConfig;
use crate::file_format::_helpers::columns::{is_semi_structured, parquet_kind};
use crate::file_format::models::ParquetKind;

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// One column's values, built row by row.
enum ColumnBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Timestamp(TimestampMillisecondBuilder),
    Date(Date32Builder),
    Text { builder: StringBuilder, json: bool },
}

impl ColumnBuilder {
    fn new(column: &ColumnConfig) -> Self {
        match parquet_kind(column) {
            ParquetKind::Int32 => Self::Int32(Int32Builder::new()),
            ParquetKind::Int64 => Self::Int64(Int64Builder::new()),
            ParquetKind::Timestamp => {
                Self::Timestamp(TimestampMillisecondBuilder::new().with_timezone("UTC"))
            }
            ParquetKind::Date => Self::Date(Date32Builder::new()),
            ParquetKind::Text => Self::Text {
                builder: StringBuilder::new(),
                json: is_semi_structured(&column.sql_type),
            },
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Timestamp(_) => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            Self::Date(_) => DataType::Date32,
            Self::Text { .. } => DataType::Utf8,
        }
    }

    fn push(&mut self, value: Option<&Value>) {
        let value = value.filter(|value| !value.is_null());
        match self {
            Self::Int32(builder) => {
                builder.append_option(integer(value).and_then(|number| i32::try_from(number).ok()));
            }
            Self::Int64(builder) => builder.append_option(integer(value)),
            Self::Timestamp(builder) => builder.append_option(value.and_then(instant_ms)),
            Self::Date(builder) => builder.append_option(value.and_then(epoch_days)),
            Self::Text { builder, json } => builder.append_option(value.map(|value| text(value, *json))),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Timestamp(builder) => Arc::new(builder.finish().with_timezone("UTC")),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::Text { builder, .. } => Arc::new(builder.finish()),
        }
    }
}

/// A zstd Parquet file of `lines` (JSON objects keyed by column name), one column per envelope column.
pub(crate) fn parquet_file(columns: &[ColumnConfig], lines: &[&str]) -> Result<Vec<u8>, String> {
    let mut builders: Vec<ColumnBuilder> = columns.iter().map(ColumnBuilder::new).collect();
    for line in lines {
        let row: Map<String, Value> =
            serde_json::from_str(line).map_err(|error| format!("row is not a JSON object: {error}"))?;
        for (column, builder) in columns.iter().zip(builders.iter_mut()) {
            builder.push(row.get(&column.name));
        }
    }
    let fields: Vec<Field> = columns
        .iter()
        .zip(&builders)
        .map(|(column, builder)| Field::new(&column.name, builder.data_type(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = builders.iter_mut().map(ColumnBuilder::finish).collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|error| error.to_string())?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut output: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, schema, Some(properties))
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(output)
}

fn integer(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

/// Milliseconds since the epoch from RFC 3339 text or a number of milliseconds.
fn instant_ms(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|instant| instant.timestamp_millis()),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

/// Days since 1970-01-01 from `YYYY-MM-DD`.
fn epoch_days(value: &Value) -> Option<i32> {
    use chrono::Datelike;
    let text = value.as_str()?;
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .map(|date| date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE)
}

/// Text as is; other values as compact JSON, and every value as JSON in a semi-structured column.
fn text(value: &Value, json: bool) -> String {
    match value {
        Value::String(text) if !json => text.clone(),
        other => other.to_string(),
    }
}
