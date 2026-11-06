use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, NullArray, TimestampMicrosecondArray};
use chrono::DateTime;

use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::row_batches::constants::{INT_DIGITS, MIN_TIMESTAMP_LENGTH, SECONDS_PER_DAY, UTC_ZONE};
use crate::row_batches::models::Cell;

/// The type `cells` suggest, by the files sources' rules (`None` when every cell is NULL), and the cells as UTC microseconds when that type is `timestamp_tz`; a decimal column holds at least the 19 digits of any 64-bit integer before the point (fewer when its scale leaves less room in 38 digits) at the largest scale seen, so a later batch's larger scale can still widen it.
pub(crate) fn column_type(cells: &[Cell]) -> (Option<ColumnType>, Vec<Option<i64>>) {
    let mut seen: Option<ColumnType> = None;
    let mut micros: Vec<Option<i64>> = Vec::new();
    for (row, cell) in cells.iter().enumerate() {
        let parsed = match cell {
            Cell::Text(text) => timestamp_micros(text),
            _ => None,
        };
        let Some(value_type) = cell_type(cell, parsed.is_some()) else {
            continue;
        };
        let merged = seen.map_or(value_type.clone(), |seen| merged_type(seen, value_type));
        match merged {
            ColumnType::Text => return (Some(ColumnType::Text), Vec::new()),
            ColumnType::TimestampTz => {
                micros.resize(row, None);
                micros.push(parsed);
            }
            _ => {}
        }
        seen = Some(merged);
    }
    micros.resize(cells.len(), None);
    let widest = seen.map(|column_type| match column_type {
        ColumnType::Decimal { precision, scale } => decimal(
            precision
                .saturating_sub(scale)
                .max(INT_DIGITS.min(MAX_DECIMAL_PRECISION.saturating_sub(scale))),
            scale,
        ),
        other => other,
    });
    (widest, micros)
}

/// The type for a column of `seen` and then `value`: decimals take the larger scale, and an int joins a decimal, which stays exact; a decimal too wide for 38 digits, or any other mix, is as `combined_type` says.
fn merged_type(seen: ColumnType, value: ColumnType) -> ColumnType {
    match (seen, value) {
        (
            ColumnType::Decimal {
                precision: left,
                scale: left_scale,
            },
            ColumnType::Decimal {
                precision: right,
                scale: right_scale,
            },
        ) => decimal(
            left.saturating_sub(left_scale)
                .max(right.saturating_sub(right_scale)),
            left_scale.max(right_scale),
        ),
        (ColumnType::Decimal { precision, scale }, ColumnType::Int)
        | (ColumnType::Int, ColumnType::Decimal { precision, scale }) => {
            decimal(precision.saturating_sub(scale).max(INT_DIGITS), scale)
        }
        (seen, value) => combined_type(seen, value),
    }
}

/// The narrowest decimal for values of up to `integer` digits before the point and `scale` after it, or `text` when they need more than 38 digits.
fn decimal(integer: u8, scale: u8) -> ColumnType {
    match u16::from(integer) + u16::from(scale) <= u16::from(MAX_DECIMAL_PRECISION) {
        true => ColumnType::Decimal {
            precision: integer + scale,
            scale,
        },
        false => ColumnType::Text,
    }
}

/// The type one decimal cell needs: its own digits before and after the point.
fn decimal_type(text: &str) -> ColumnType {
    let digits = text.trim_start_matches(['-', '+']);
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let integer = integer.trim_start_matches('0').len();
    match (u8::try_from(integer), u8::try_from(fraction.len())) {
        (Ok(integer), Ok(scale)) => decimal(integer, scale),
        _ => ColumnType::Text,
    }
}

/// `text`, fixed-point decimal digits, as an integer at `scale` digits after the point.
fn scaled(text: &str, scale: u8) -> Option<i128> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.trim_start_matches('+')),
    };
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let padding = usize::from(scale).checked_sub(fraction.len())?;
    match format!("{integer}{fraction}{}", "0".repeat(padding)).parse::<i128>() {
        Ok(value) if negative => Some(-value),
        Ok(value) => Some(value),
        Err(_) => None,
    }
}

/// The cell for a JSON number's text: an `int` when it fits 64 bits, a `float` when it is finite, else text.
pub(crate) fn number_cell(text: &str) -> Cell {
    match text.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
        false => text
            .parse::<i64>()
            .map_or_else(|_| Cell::Number(text.to_owned()), Cell::Int),
        true => match text.parse::<f64>() {
            Ok(value) if value.is_finite() => Cell::Float(value),
            Ok(_) | Err(_) => Cell::Number(text.to_owned()),
        },
    }
}

/// `cells` as an Arrow array of `column_type` (a `NullArray` for `None`), taking timestamps from `micros`; the cells already fit the type.
pub(crate) fn column_array(
    cells: Vec<Cell>,
    column_type: Option<&ColumnType>,
    micros: Vec<Option<i64>>,
) -> ArrayRef {
    match column_type {
        None => Arc::new(NullArray::new(cells.len())),
        Some(ColumnType::Bool) => {
            let mut builder = BooleanBuilder::with_capacity(cells.len());
            cells.iter().for_each(|cell| match cell {
                Cell::Bool(value) => builder.append_value(*value),
                _ => builder.append_null(),
            });
            Arc::new(builder.finish())
        }
        Some(ColumnType::Int) => {
            let mut builder = Int64Builder::with_capacity(cells.len());
            cells.iter().for_each(|cell| match cell {
                Cell::Int(value) => builder.append_value(*value),
                _ => builder.append_null(),
            });
            Arc::new(builder.finish())
        }
        Some(ColumnType::Float) => {
            let mut builder = Float64Builder::with_capacity(cells.len());
            cells.iter().for_each(|cell| match cell {
                Cell::Int(value) => builder.append_value(*value as f64),
                Cell::Float(value) => builder.append_value(*value),
                _ => builder.append_null(),
            });
            Arc::new(builder.finish())
        }
        Some(ColumnType::TimestampTz) => {
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone(UTC_ZONE))
        }
        Some(ColumnType::TimestampNtz) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(cells.len());
            cells.iter().for_each(|cell| match cell {
                Cell::TimestampNtz(value) => builder.append_value(*value),
                _ => builder.append_null(),
            });
            Arc::new(builder.finish())
        }
        Some(ColumnType::Date) => {
            let mut builder = Date32Builder::with_capacity(cells.len());
            cells.iter().for_each(|cell| match cell {
                Cell::Date(value) => builder.append_value(*value),
                _ => builder.append_null(),
            });
            Arc::new(builder.finish())
        }
        Some(ColumnType::Decimal { precision, scale }) => {
            let mut builder = Decimal128Builder::with_capacity(cells.len());
            cells.iter().for_each(|cell| {
                let value = match cell {
                    Cell::Decimal(text) => scaled(text, *scale),
                    Cell::Int(value) => scaled(&value.to_string(), *scale),
                    _ => None,
                };
                builder.append_option(value);
            });
            let arrow_scale = i8::try_from(*scale).unwrap_or(i8::MAX);
            match builder
                .finish()
                .with_precision_and_scale(*precision, arrow_scale)
            {
                Ok(array) => Arc::new(array),
                Err(_) => text_array(cells),
            }
        }
        Some(_) => text_array(cells),
    }
}

fn text_array(cells: Vec<Cell>) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(cells.len(), cells.len() * 16);
    for cell in cells {
        match cell {
            Cell::Null => builder.append_null(),
            Cell::Bool(value) => builder.append_value(if value { "true" } else { "false" }),
            Cell::Int(value) => builder.append_value(value.to_string()),
            Cell::Float(value) => {
                builder.append_value(float_text(value));
            }
            Cell::Number(text) | Cell::Text(text) | Cell::Json(text) | Cell::Decimal(text) => {
                builder.append_value(text);
            }
            Cell::Date(days) => builder.append_value(
                chrono::DateTime::from_timestamp(i64::from(days) * SECONDS_PER_DAY, 0)
                    .map(|time| time.date_naive().to_string())
                    .unwrap_or_default(),
            ),
            Cell::TimestampNtz(value) => builder.append_value(
                chrono::DateTime::from_timestamp_micros(value)
                    .map(|time| time.naive_utc().format("%Y-%m-%dT%H:%M:%S%.f").to_string())
                    .unwrap_or_default(),
            ),
        }
    }
    Arc::new(builder.finish())
}

/// The type one cell suggests (`None` for NULL); `timestamp` says a text cell is an RFC 3339 timestamp.
fn cell_type(cell: &Cell, timestamp: bool) -> Option<ColumnType> {
    match cell {
        Cell::Null => None,
        Cell::Bool(_) => Some(ColumnType::Bool),
        Cell::Int(_) => Some(ColumnType::Int),
        Cell::Float(_) => Some(ColumnType::Float),
        Cell::Number(_) => Some(ColumnType::Text),
        Cell::Text(_) if timestamp => Some(ColumnType::TimestampTz),
        Cell::Text(_) => Some(ColumnType::Text),
        Cell::Json(_) => Some(ColumnType::Json),
        Cell::Date(_) => Some(ColumnType::Date),
        Cell::TimestampNtz(_) => Some(ColumnType::TimestampNtz),
        Cell::Decimal(text) => Some(decimal_type(text)),
    }
}

/// `text` as UTC microseconds when it is an RFC 3339 timestamp with an offset, the files sources' rule for `timestamp_tz`.
fn timestamp_micros(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let shaped = bytes.len() >= MIN_TIMESTAMP_LENGTH
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && matches!(bytes[10], b'T' | b't' | b' ');
    match shaped {
        true => match DateTime::parse_from_rfc3339(text) {
            Ok(time) => Some(time.timestamp_micros()),
            Err(_) => None,
        },
        false => None,
    }
}

/// `value` as the shortest text that reads back as the same float (`1.5`, `1e20`), as JSON writes it.
fn float_text(value: f64) -> String {
    match serde_json::Number::from_f64(value) {
        Some(number) => number.to_string(),
        None => value.to_string(),
    }
}
