use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, NullArray, TimestampMicrosecondArray};
use chrono::DateTime;

use crate::column_types::main::combined_type::combined_type;
use crate::column_types::models::ColumnType;
use crate::row_batches::constants::{MIN_TIMESTAMP_LENGTH, UTC_ZONE};
use crate::row_batches::models::Cell;

/// The type `cells` suggest, by the files sources' rules (`None` when every cell is NULL), and the cells as UTC microseconds when that type is `timestamp_tz`.
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
        let merged = seen.map_or(value_type.clone(), |seen| combined_type(seen, value_type));
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
    (seen, micros)
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
            Cell::Number(text) | Cell::Text(text) | Cell::Json(text) => builder.append_value(text),
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
