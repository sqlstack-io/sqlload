//! Read an Excel worksheet's rows as typed cells.

use std::io::Cursor;

use calamine::{Data, ExcelDateTime, Reader, Xlsx};
use chrono::NaiveTime;

use crate::column_types::models::ColumnType;
use crate::files_source::constants::{
    EXCEL_DAY, MAX_EXACT_FLOAT_INT, NORMAL_DATE, NORMAL_TIME, NORMAL_TIMESTAMP,
};
use crate::files_source::models::{Cell, CellRecord, XlsxOptions};

/// The column names and rows of `options`' worksheet in `workbook`, each row numbered as in the sheet; empty rows are left out, and so is a column with an empty header and no values.
pub(crate) fn xlsx_records(
    workbook: Vec<u8>,
    options: &XlsxOptions,
) -> Result<(Vec<String>, Vec<CellRecord>), String> {
    let mut book = Xlsx::new(Cursor::new(workbook))
        .map_err(|error| format!("cannot read the workbook: {error}"))?;
    let sheets = book.sheet_names();
    let sheet = match &options.sheet {
        Some(sheet) if sheets.contains(sheet) => sheet.clone(),
        Some(sheet) => {
            return Err(format!(
                "the workbook has no sheet {sheet:?}; its sheets are {sheets:?}"
            ));
        }
        None => sheets
            .first()
            .cloned()
            .ok_or_else(|| "the workbook has no sheets".to_owned())?,
    };
    let range = book
        .worksheet_range(&sheet)
        .map_err(|error| format!("sheet {sheet:?}: {error}"))?;
    let (top, left) = range.start().unwrap_or_default();
    let mut rows = range
        .rows()
        .enumerate()
        .map(|(index, cells)| (u64::from(top) + index as u64 + 1, cells))
        .filter(|(row, cells)| {
            *row > u64::from(options.skip_rows) && cells.iter().any(|cell| cell_of(cell).is_some())
        });
    let width = range.width();
    let place = |position: usize| format!("column_{}", u64::from(left) + position as u64 + 1);
    let mut names: Vec<String> = (0..width).map(place).collect();
    let mut named = vec![false; width];
    if options.header
        && let Some((row, cells)) = rows.next()
    {
        for (position, cell) in cells.iter().enumerate() {
            let Some(cell) = cell_of(cell) else {
                continue;
            };
            if names.contains(&cell.text) {
                return Err(format!(
                    "row {row}: column {} appears twice in the header",
                    cell.text
                ));
            }
            names[position] = cell.text;
            named[position] = true;
        }
    }
    let mut used = named;
    let mut records: Vec<CellRecord> = Vec::new();
    for (row, cells) in rows {
        let mut record = CellRecord {
            line: row,
            cells: Vec::new(),
        };
        for (position, cell) in cells.iter().enumerate() {
            if let Some(cell) = cell_of(cell) {
                used[position] = true;
                record.cells.push((names[position].clone(), cell));
            }
        }
        records.push(record);
    }
    let names = names
        .into_iter()
        .zip(used)
        .filter_map(|(name, used)| used.then_some(name))
        .collect();
    Ok((names, records))
}

/// A cell's text and the type it suggests; `None` for an empty cell or an error value such as `#N/A`, which load as NULL.
fn cell_of(data: &Data) -> Option<Cell> {
    let cell = |text: String, kind: ColumnType| Some(Cell { text, kind });
    match data {
        Data::Empty | Data::Error(_) => None,
        Data::Int(value) => cell(value.to_string(), ColumnType::Int),
        Data::Float(value) => Some(number_cell(*value)),
        Data::Bool(value) => cell(value.to_string(), ColumnType::Bool),
        Data::String(text) | Data::DateTimeIso(text) | Data::DurationIso(text) => {
            cell(text.clone(), ColumnType::Text)
        }
        Data::DateTime(value) => Some(date_cell(value)),
    }
}

/// A number: Excel stores every number as a float, so a whole one within a float's exact range is an `int`.
fn number_cell(value: f64) -> Cell {
    match value.trunc() == value && value.abs() < MAX_EXACT_FLOAT_INT {
        true => Cell {
            text: format!("{value:.0}"),
            kind: ColumnType::Int,
        },
        false => Cell {
            text: value.to_string(),
            kind: ColumnType::Float,
        },
    }
}

/// A date-formatted number: a `time` under one day, a `date` at midnight, otherwise a `timestamp_ntz`; a duration is text, as ISO 8601.
fn date_cell(value: &ExcelDateTime) -> Cell {
    let moment = value.as_datetime().filter(|_| value.is_datetime());
    match (moment, value.as_duration()) {
        (Some(moment), _) if value.as_f64() < EXCEL_DAY => Cell {
            text: moment.time().format(NORMAL_TIME).to_string(),
            kind: ColumnType::Time,
        },
        (Some(moment), _) if moment.time() == NaiveTime::MIN => Cell {
            text: moment.date().format(NORMAL_DATE).to_string(),
            kind: ColumnType::Date,
        },
        (Some(moment), _) => Cell {
            text: moment.format(NORMAL_TIMESTAMP).to_string(),
            kind: ColumnType::TimestampNtz,
        },
        (None, Some(duration)) => Cell {
            text: duration.to_string(),
            kind: ColumnType::Text,
        },
        (None, None) => number_cell(value.as_f64()),
    }
}
