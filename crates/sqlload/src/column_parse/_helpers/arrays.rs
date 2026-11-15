//! Parsing one Arrow column into a `timestamp_tz` or `date` column.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{ArrayRef, Date32Array, RecordBatch, TimestampMicrosecondArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono_tz::Tz;

use crate::column_parse::_helpers::value::{raw_shown, value_time};
use crate::column_parse::constants::UTC_ZONE;
use crate::column_parse::models::{BadValue, ColumnParse, ParseKind, ParsedTime, RawTime};
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN};

/// Column `name` of a batch (`array`) parsed by `parse`, a time without an offset in `zone` unless `parse` has its own `tz`; a NULL stays NULL. A value no format reads fails, naming its row (`label`), the column and the value, or loads NULL under `on_error = "null"`. Instants already (text with an offset, which the crossing reads as `timestamp_tz`) stay as they are under `timestamp`, as `tz` would not apply to them.
pub(crate) fn parsed_array(
    array: &ArrayRef,
    (name, parse): (&str, &ColumnParse),
    zone: Tz,
    label: &dyn Fn(usize) -> String,
) -> Result<ArrayRef, String> {
    if let (ParseKind::Timestamp, DataType::Timestamp(_, Some(_))) = (parse.kind, array.data_type())
    {
        let instants = DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_ZONE.into()));
        return cast_with_options(array, &instants, &CastOptions::default())
            .map_err(|error| format!("column {name}: {error}"));
    }
    let read = readable(array, name)?;
    let mut parsed: Vec<Option<ParsedTime>> = Vec::with_capacity(read.len());
    for index in 0..read.len() {
        let Some(raw) = raw_at(&read, index) else {
            parsed.push(None);
            continue;
        };
        match (value_time(raw, parse, zone), parse.on_error) {
            (Ok(time), _) => parsed.push(Some(time)),
            (Err(_), BadValue::Null) => parsed.push(None),
            (Err(reason), BadValue::Fail) => {
                return Err(format!(
                    "{}: column {name} holds {}, {reason}; fix the value, or set on_error = \"null\" in its parse to load NULL",
                    label(index),
                    raw_shown(raw)
                ));
            }
        }
    }
    Ok(built(&parsed, parse.kind))
}

/// Each row's name in an error: its file and row in the file when the batch carries them (a files source's), else its row in the read, after `rows_before` rows.
pub(crate) fn row_labels(batch: &RecordBatch, rows_before: u64) -> impl Fn(usize) -> String {
    let file = column_as(batch, SOURCE_FILE_COLUMN, &DataType::Utf8);
    let row = column_as(batch, SOURCE_ROW_COLUMN, &DataType::Int64);
    move |index| match (&file, &row) {
        (Some(file), Some(row)) if file.is_valid(index) && row.is_valid(index) => format!(
            "{} row {}",
            file.as_string::<i32>().value(index),
            row.as_primitive::<Int64Type>().value(index)
        ),
        _ => format!("row {}", rows_before + index as u64 + 1),
    }
}

/// Column `name` of `batch` as `data_type`; `None` when the batch has no such column or it doesn't cast, and the rows are then named by their place in the read.
fn column_as(batch: &RecordBatch, name: &str, data_type: &DataType) -> Option<ArrayRef> {
    let column = batch.column_by_name(name)?;
    match cast_with_options(column, data_type, &CastOptions::default()) {
        Ok(cast) => Some(cast),
        Err(_) => None,
    }
}

/// `array` as text, 64-bit integers or floats, which `parse` reads; a time or date column is already parsed, and anything else holds no times.
fn readable(array: &ArrayRef, name: &str) -> Result<ArrayRef, String> {
    let target = match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
            DataType::Utf8
        }
        integer if integer.is_integer() => DataType::Int64,
        float if float.is_floating() => DataType::Float64,
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => {
            return Err(format!(
                "column {name} is already a timestamp or a date, so parse has no text to read; drop its parse"
            ));
        }
        other => {
            return Err(format!(
                "column {name} holds {other} values; parse reads text, or numbers with an epoch format"
            ));
        }
    };
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    cast_with_options(array, &target, &options).map_err(|error| format!("column {name}: {error}"))
}

/// The value at `index` of a column `readable` gave, `None` when NULL.
fn raw_at(read: &ArrayRef, index: usize) -> Option<RawTime<'_>> {
    if read.is_null(index) {
        return None;
    }
    Some(match read.data_type() {
        DataType::Int64 => RawTime::Int(read.as_primitive::<Int64Type>().value(index)),
        DataType::Float64 => RawTime::Float(read.as_primitive::<Float64Type>().value(index)),
        _ => RawTime::Text(read.as_string::<i32>().value(index)),
    })
}

/// The parsed values as a `timestamp_tz` (UTC) or `date` column.
fn built(parsed: &[Option<ParsedTime>], kind: ParseKind) -> ArrayRef {
    match kind {
        ParseKind::Timestamp => {
            let micros: Vec<Option<i64>> = parsed
                .iter()
                .map(|time| match time {
                    Some(ParsedTime::Instant(micros)) => Some(*micros),
                    _ => None,
                })
                .collect();
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone(UTC_ZONE))
        }
        ParseKind::Date => {
            let days: Vec<Option<i32>> = parsed
                .iter()
                .map(|time| match time {
                    Some(ParsedTime::Day(days)) => Some(*days),
                    _ => None,
                })
                .collect();
            Arc::new(Date32Array::from(days))
        }
    }
}

/// `batch`'s fields and columns, each column `parses` names parsed (see [`parsed_array`]), the others as they are.
pub(crate) fn parsed_columns(
    batch: &RecordBatch,
    parses: &BTreeMap<String, ColumnParse>,
    zone: Tz,
    label: &dyn Fn(usize) -> String,
) -> Result<(Vec<Field>, Vec<ArrayRef>), String> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let Some(parse) = parses.get(field.name()) else {
            fields.push(field.as_ref().clone());
            arrays.push(Arc::clone(array));
            continue;
        };
        let parsed = parsed_array(array, (field.name(), parse), zone, label)?;
        fields.push(Field::new(field.name(), parsed.data_type().clone(), true));
        arrays.push(parsed);
    }
    Ok((fields, arrays))
}
