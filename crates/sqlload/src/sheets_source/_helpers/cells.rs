//! Cells as the Sheets API writes them, and the text each loads as.

use chrono::{DateTime, NaiveDateTime};
use serde_json::Value;

use crate::column_types::models::ColumnType;
use crate::sheets_source::constants::{DAY_MS, UNIX_EPOCH_SERIAL, WHOLE_TEXT_LIMIT};
use crate::sheets_source::models::{Cell, Grid, NumberKind};

/// A cell of a `rowData` entry: its effective value, with a number's format type and displayed text.
pub(crate) fn parse_cell(cell: &Value) -> Cell {
    let value = &cell["effectiveValue"];
    let shown = cell["formattedValue"].as_str().unwrap_or_default();
    let kind = match cell["effectiveFormat"]["numberFormat"]["type"].as_str() {
        Some("DATE") => NumberKind::Date,
        Some("DATE_TIME") => NumberKind::DateTime,
        Some("TIME") => NumberKind::Time,
        Some("CURRENCY" | "PERCENT" | "SCIENTIFIC") => NumberKind::Fractional,
        _ => NumberKind::Plain,
    };
    match (
        value["boolValue"].as_bool(),
        value["numberValue"].as_f64(),
        value["stringValue"].as_str(),
        value.get("errorValue"),
    ) {
        (Some(flag), _, _, _) => Cell::Bool(flag),
        (_, Some(number), _, _) => Cell::Number {
            value: number,
            kind,
            shown: shown.to_owned(),
        },
        (_, _, Some(text), _) => Cell::Text(text.to_owned()),
        (_, _, _, Some(_)) => Cell::Error(shown.to_owned()),
        _ => Cell::Empty,
    }
}

/// The date and time a serial number stands for (days since 1899-12-30), to the millisecond.
fn serial_time(serial: f64) -> Option<NaiveDateTime> {
    let ms = (serial * DAY_MS).round() as i64 - UNIX_EPOCH_SERIAL * DAY_MS as i64;
    DateTime::from_timestamp_millis(ms).map(|time| time.naive_utc())
}

/// The text a cell loads from for a column of `column_type`, which Arrow then converts: the displayed text for text columns, a canonical form of the value otherwise; `Err` for an error cell.
pub(crate) fn cell_text(cell: &Cell, column_type: &ColumnType) -> Result<Option<String>, String> {
    let as_text = matches!(
        column_type,
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Native(_)
    );
    Ok(match cell {
        Cell::Empty => None,
        Cell::Error(shown) => return Err(format!("holds the error {shown}")),
        Cell::Text(text) => Some(text.clone()),
        Cell::Bool(flag) if as_text => Some(if *flag { "TRUE" } else { "FALSE" }.to_owned()),
        Cell::Bool(flag) => Some(flag.to_string()),
        Cell::Number { shown, .. } if as_text => Some(shown.clone()),
        Cell::Number { value, kind, .. } => Some(number_text(*value, *kind, column_type)),
    })
}

/// A number as the text of its value: a date, a timestamp or a time of day for dated cells (by the column's type, so a date loads into a timestamp column at midnight), a whole number without a fraction, any other number as it is.
fn number_text(value: f64, kind: NumberKind, column_type: &ColumnType) -> String {
    let time = serial_time(value);
    match (kind, column_type, time) {
        (NumberKind::Plain | NumberKind::Fractional, _, _) | (_, _, None) => plain_number(value),
        (_, ColumnType::Date, Some(time)) => time.format("%Y-%m-%d").to_string(),
        (_, ColumnType::Time, Some(time)) => time.format("%H:%M:%S%.3f").to_string(),
        (_, ColumnType::TimestampTz | ColumnType::TimestampNtz, Some(time)) => {
            time.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
        }
        (_, _, Some(_)) => plain_number(value),
    }
}

/// A number's text: no fraction when it is whole.
fn plain_number(value: f64) -> String {
    match value.trunc() == value && value.abs() < WHOLE_TEXT_LIMIT {
        true => format!("{value:.0}"),
        false => value.to_string(),
    }
}

/// One tab's cells from its entry in the answer: its first range's start and rows, each cut after its last non-empty cell.
pub(crate) fn parse_grid(sheet: &Value) -> Grid {
    let data = &sheet["data"][0];
    let rows: Vec<Vec<Cell>> = data["rowData"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| {
            let mut cells: Vec<Cell> = row["values"]
                .as_array()
                .into_iter()
                .flatten()
                .map(parse_cell)
                .collect();
            let width = cells
                .iter()
                .rposition(|cell| *cell != Cell::Empty)
                .map_or(0, |last| last + 1);
            cells.truncate(width);
            cells
        })
        .collect();
    Grid {
        title: sheet["properties"]["title"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        start_row: data["startRow"].as_u64().unwrap_or_default() as u32,
        start_column: data["startColumn"].as_u64().unwrap_or_default() as u32,
        rows,
    }
}
