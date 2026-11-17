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
use crate::time_text::main::parse_date::parse_date;
use crate::time_text::main::parse_instant::parse_instant;

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
    fn new(column: &ColumnConfig, kind: ParquetKind) -> Self {
        match kind {
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

    /// Append `value`, or NULL for none; a value the column's type cannot hold is an error naming it, never a silent NULL.
    fn push(&mut self, value: Option<&Value>) -> Result<(), String> {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            self.push_null();
            return Ok(());
        };
        let refused = |kind: &str| format!("{value} is not {kind}");
        match self {
            Self::Int32(builder) => builder.append_value(
                integer(value)
                    .and_then(narrow)
                    .ok_or_else(|| refused("a 32-bit integer"))?,
            ),
            Self::Int64(builder) => {
                builder.append_value(integer(value).ok_or_else(|| refused("an integer"))?);
            }
            Self::Timestamp(builder) => {
                builder.append_value(instant_ms(value).ok_or_else(|| refused("a timestamp"))?);
            }
            Self::Date(builder) => {
                builder.append_value(epoch_days(value).ok_or_else(|| refused("a date"))?);
            }
            Self::Text { builder, json } => builder.append_value(text(value, *json)),
        }
        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Timestamp(builder) => builder.append_null(),
            Self::Date(builder) => builder.append_null(),
            Self::Text { builder, .. } => builder.append_null(),
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

/// A zstd Parquet file of `lines` (JSON objects keyed by column name), one column per envelope column, typed by the field it holds.
pub(crate) fn parquet_file(columns: &[ColumnConfig], lines: &[&str]) -> Result<Vec<u8>, String> {
    let kinds: Vec<ParquetKind> = columns.iter().map(parquet_kind).collect();
    parquet_bytes(&record_batch(columns, &kinds, lines)?)
}

/// `lines` (JSON objects keyed by column name) as one Arrow batch, each column built as its `kinds` entry; a value its column cannot hold is an error naming the column.
pub(crate) fn record_batch(
    columns: &[ColumnConfig],
    kinds: &[ParquetKind],
    lines: &[&str],
) -> Result<RecordBatch, String> {
    let mut builders: Vec<ColumnBuilder> = columns
        .iter()
        .zip(kinds)
        .map(|(column, kind)| ColumnBuilder::new(column, *kind))
        .collect();
    for line in lines {
        let row: Map<String, Value> = serde_json::from_str(line)
            .map_err(|error| format!("row is not a JSON object: {error}"))?;
        for (column, builder) in columns.iter().zip(builders.iter_mut()) {
            builder
                .push(row.get(&column.name))
                .map_err(|reason| format!("column {}: {reason}", column.name))?;
        }
    }
    let fields: Vec<Field> = columns
        .iter()
        .zip(&builders)
        .map(|(column, builder)| Field::new(&column.name, builder.data_type(), true))
        .collect();
    let arrays: Vec<ArrayRef> = builders.iter_mut().map(ColumnBuilder::finish).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

/// `batch` as a zstd Parquet file.
pub(crate) fn parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut output: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(output)
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => match text.trim().parse::<i64>() {
            Ok(number) => Some(number),
            Err(_) => None,
        },
        _ => None,
    }
}

fn narrow(number: i64) -> Option<i32> {
    match i32::try_from(number) {
        Ok(narrowed) => Some(narrowed),
        Err(_) => None,
    }
}

/// Milliseconds since the epoch from text [`parse_instant`] reads or a number of milliseconds.
fn instant_ms(value: &Value) -> Option<i64> {
    match value {
        Value::String(text) => parse_instant(text).map(|instant| instant.timestamp_millis()),
        Value::Number(number) => number.as_i64(),
        _ => None,
    }
}

/// Days since 1970-01-01 from text [`parse_date`] reads.
fn epoch_days(value: &Value) -> Option<i32> {
    use chrono::Datelike;
    parse_date(value.as_str()?).map(|date| date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE)
}

/// Text as is; other values as compact JSON, and every value as JSON in a semi-structured column.
fn text(value: &Value, json: bool) -> String {
    match value {
        Value::String(text) if !json => text.clone(),
        other => other.to_string(),
    }
}
