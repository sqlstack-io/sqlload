//! Read an Excel worksheet's rows as typed cells.

use std::collections::BTreeMap;
use std::io::Cursor;

use calamine::{Data, ExcelDateTime, Reader, Xlsx, XlsxError};
use chrono::NaiveTime;

use crate::column_types::models::ColumnType;
use crate::files_source::constants::{
    EXCEL_DAY, MAX_EXACT_FLOAT_INT, MAX_SHEET_CELLS, NORMAL_DATE, NORMAL_TIME, NORMAL_TIMESTAMP,
};
use crate::files_source::models::{Cell, CellRecord, XlsxOptions};
use crate::sheets_source::main::a1_range::a1_range;
use crate::sheets_source::models::A1Range;

/// The column names and rows of `options`' worksheet in `workbook`, each row numbered as in the sheet; empty rows are left out, and so is a column with an empty header and no values. Only the cells of `options.range` are read, when it is set. Cells are read one at a time, so a sheet's stated size is never allocated; a sheet over `MAX_SHEET_CELLS` values is an error.
pub(crate) fn xlsx_records(
    workbook: Vec<u8>,
    options: &XlsxOptions,
) -> Result<(Vec<String>, Vec<CellRecord>), String> {
    let range = options.range.as_deref().map(a1_range).transpose()?;
    let wanted = range
        .as_ref()
        .and_then(|range| range.sheet.clone())
        .or_else(|| options.sheet.clone());
    let mut book = Xlsx::new(Cursor::new(workbook))
        .map_err(|error| format!("cannot read the workbook: {error}"))?;
    let sheets = book.sheet_names();
    let sheet = match wanted {
        Some(sheet) if sheets.contains(&sheet) => sheet,
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
    let first_row = range
        .as_ref()
        .map_or(u64::from(options.skip_rows) + 1, |range| {
            u64::from(range.first_row)
        });
    let last_row = range
        .as_ref()
        .and_then(|range| range.last_row)
        .map_or(u64::MAX, u64::from);
    let columns = range
        .as_ref()
        .map_or(0..=u32::MAX, |range| range.first_column..=range.last_column);
    let first_column = *columns.start();
    let header_row = options
        .header_row
        .map(|header_row| first_row + u64::from(header_row) - 1);
    let sheet_error = |error: XlsxError| format!("sheet {sheet:?}: {error}");
    let mut reader = book.worksheet_cells_reader(&sheet).map_err(sheet_error)?;
    let mut rows: BTreeMap<u64, Vec<(u32, Cell)>> = BTreeMap::new();
    let mut count = 0_usize;
    while let Some(found) = reader.next_cell().map_err(sheet_error)? {
        let (row, column) = found.get_position();
        let row = u64::from(row) + 1;
        if row > last_row {
            break;
        }
        if row < header_row.unwrap_or(first_row) || !columns.contains(&column) {
            continue;
        }
        let Some(cell) = cell_of(&Data::from(found.get_value().clone())) else {
            continue;
        };
        count += 1;
        if count > MAX_SHEET_CELLS {
            return Err(format!(
                "sheet {sheet:?} holds over {MAX_SHEET_CELLS} values; SQLLoad reads a worksheet whole, up to that many"
            ));
        }
        rows.entry(row)
            .or_default()
            .push((column - first_column, cell));
    }
    if let Some(header_row) = header_row.filter(|_| options.header)
        && rows
            .first_key_value()
            .is_some_and(|(row, _)| *row != header_row)
    {
        return Err(format!(
            "sheet {sheet:?}: the header row, sheet row {header_row} (header_row {}), is empty; set header_row, or header = false",
            options.header_row.unwrap_or(1)
        ));
    }
    let mut rows = rows.into_iter();
    let mut headers: BTreeMap<u32, String> = BTreeMap::new();
    if options.header
        && let Some((row, cells)) = rows.next()
    {
        for (column, cell) in cells {
            if headers.values().any(|name| *name == cell.text) {
                return Err(format!(
                    "row {row}: column {} appears twice in the header",
                    cell.text
                ));
            }
            headers.insert(column, cell.text);
        }
    }
    let rows: Vec<(u64, Vec<(u32, Cell)>)> = rows.collect();
    let mut names = match (&options.column_names, options.header) {
        (Some(given), false) => given_names(given, range.as_ref(), &rows)?,
        _ => headers,
    };
    let mut records: Vec<CellRecord> = Vec::with_capacity(rows.len());
    for (row, cells) in rows {
        let mut record = CellRecord {
            line: row,
            cells: Vec::with_capacity(cells.len()),
        };
        for (column, cell) in cells {
            let name = match names.get(&column) {
                Some(name) => name.clone(),
                None => {
                    let name = placeholder(&names, column, row)?;
                    names.insert(column, name.clone());
                    name
                }
            };
            record.cells.push((name, cell));
        }
        records.push(record);
    }
    Ok((names.into_values().collect(), records))
}

/// `given` as the names of the columns read, by place: as many as the range spans, or without a range, as many as reach the rightmost value.
fn given_names(
    given: &[String],
    range: Option<&A1Range>,
    rows: &[(u64, Vec<(u32, Cell)>)],
) -> Result<BTreeMap<u32, String>, String> {
    let mut width = match range {
        Some(range) => range.width(),
        None if rows.is_empty() => given.len(),
        None => 0,
    };
    if range.is_none() {
        for (_, cells) in rows {
            for (column, _) in cells {
                width = width.max(*column as usize + 1);
            }
        }
    }
    match width == given.len() {
        true => Ok((0_u32..).zip(given.iter().cloned()).collect()),
        false => Err(format!(
            "the {} has {width} columns, but column_names names {}; name each column",
            match range {
                Some(_) => "range",
                None => "sheet",
            },
            given.len()
        )),
    }
}

/// The name of headerless column `column`, `column_` and its place in the range (the sheet without one); a header in `names` already using that name is an error naming `row`.
fn placeholder(names: &BTreeMap<u32, String>, column: u32, row: u64) -> Result<String, String> {
    let name = format!("column_{}", u64::from(column) + 1);
    if names.values().any(|taken| *taken == name) {
        return Err(format!(
            "row {row}: the column without a header in place {} would be named {name}, which the header already uses",
            u64::from(column) + 1
        ));
    }
    Ok(name)
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
