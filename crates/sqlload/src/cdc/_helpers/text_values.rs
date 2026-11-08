//! pgoutput's text values as Arrow arrays of the columns' types.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, StringArray};
use arrow_cast::cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDateTime};

use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, WireType};

/// The Arrow type a column of `column_type` is staged as, the same as the snapshot's `COPY` reader produces.
pub(crate) fn arrow_type(column_type: &ColumnType) -> Result<DataType, String> {
    Ok(match column_type {
        ColumnType::Bool => DataType::Boolean,
        ColumnType::SmallInt => DataType::Int16,
        ColumnType::Int => DataType::Int64,
        ColumnType::Float => DataType::Float64,
        ColumnType::Decimal { precision, scale } => DataType::Decimal128(
            *precision,
            i8::try_from(*scale).map_err(|error| error.to_string())?,
        ),
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Json | ColumnType::Uuid => {
            DataType::Utf8
        }
        ColumnType::Date => DataType::Date32,
        ColumnType::Time => DataType::Time64(TimeUnit::Microsecond),
        ColumnType::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ColumnType::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::Binary => DataType::Binary,
        ColumnType::Native(name) => {
            return Err(format!("native({name:?}) has no Arrow representation"));
        }
        ColumnType::Array(_) => {
            return Err(
                "an array column is not read from a change stream yet; pin text".to_owned(),
            );
        }
    })
}

/// A Postgres text timestamp as microseconds since 1970 UTC; one without an offset is taken as UTC.
fn timestamp_micros(text: &str) -> Result<i64, String> {
    let with_offset = DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z")
        .map(|time| time.timestamp_micros());
    with_offset
        .or_else(|_| {
            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                .map(|time| time.and_utc().timestamp_micros())
        })
        .map_err(|_| format!("{text:?} is not a timestamp SQLLoad can read"))
}

/// Postgres `bytea` hex output (`\x0a0b`) as bytes.
fn bytea(text: &str) -> Result<Vec<u8>, String> {
    let problem = || format!("{text:?} is not bytea hex output");
    let hex_text = text.strip_prefix("\\x").ok_or_else(problem)?;
    hex::decode(hex_text).map_err(|_| problem())
}

/// `text` as a number; `single` (a `real` column) parses it as the 32-bit value the snapshot reads, then widens it the same way.
fn parse_float(text: &str, single: bool) -> Result<f64, String> {
    let problem = |_| format!("{text:?} is not a number");
    match single {
        true => text.parse::<f32>().map(f64::from).map_err(problem),
        false => text.parse::<f64>().map_err(problem),
    }
}

/// `values` (`None` is NULL) as an array of `column`'s type, or the first value that does not parse.
pub(crate) fn text_array(values: &[Option<&str>], column: &ReadColumn) -> Result<ArrayRef, String> {
    let data_type = arrow_type(&column.output)?;
    let single = column.wire == WireType::Float4;
    Ok(match &data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                builder.append_option(match value {
                    None => None,
                    Some("t") => Some(true),
                    Some("f") => Some(false),
                    Some(other) => return Err(format!("{other:?} is not a boolean")),
                });
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for value in values {
                builder.append_option(value.map(|text| parse_float(text, single)).transpose()?);
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(_, zone) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
            for value in values {
                builder.append_option(value.map(timestamp_micros).transpose()?);
            }
            let array = builder.finish();
            Arc::new(match zone {
                Some(zone) => array.with_timezone(zone.as_ref()),
                None => array,
            })
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(values.len(), 0);
            for value in values {
                builder.append_option(value.map(bytea).transpose()?);
            }
            Arc::new(builder.finish())
        }
        _ => {
            let text: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
            let options = CastOptions {
                safe: false,
                ..CastOptions::default()
            };
            cast_with_options(&text, &data_type, &options).map_err(|error| error.to_string())?
        }
    })
}

/// Microseconds since 1970 UTC as RFC 3339 text with microseconds.
pub(crate) fn micros_text(micros: i64) -> String {
    DateTime::from_timestamp_micros(micros)
        .map(|time| time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        .unwrap_or_default()
}
