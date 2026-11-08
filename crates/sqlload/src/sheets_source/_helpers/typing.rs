//! The types a range's cells suggest for its columns.

use crate::column_types::models::ColumnType;
use crate::sheets_source::_helpers::layout::{cell_at, cell_name};
use crate::sheets_source::constants::MAX_EXACT_INTEGER;
use crate::sheets_source::models::{Cell, Grid, NumberKind, SheetColumn};

/// A warning naming the leftmost cell right of the header's `width` columns in `rows`, which is not loaded.
pub(crate) fn beyond_warning(grid: &Grid, rows: &[usize], width: usize) -> Option<String> {
    let (row, column) = rows
        .iter()
        .filter_map(|row| first_beyond(grid, *row, width))
        .min_by_key(|(_, column)| *column)?;
    Some(format!(
        "{}: cells right of the header, such as {}, are not loaded; name their column in the header to load them",
        grid.title,
        cell_name(grid, row, column)
    ))
}

/// Range row `row` and its first column at or past `width` that holds something.
fn first_beyond(grid: &Grid, row: usize, width: usize) -> Option<(usize, usize)> {
    let offset = grid.rows[row]
        .iter()
        .skip(width)
        .position(|cell| *cell != Cell::Empty)?;
    Some((row, width + offset))
}

/// Column `name` at `position`, typed by every cell of `rows` in it; with a warning when its cells disagree so it loads as text. A column holding an error cell cannot load, naming the first one.
pub(crate) fn typed_column(
    grid: &Grid,
    rows: &[usize],
    (position, name): (usize, String),
) -> (SheetColumn, Option<String>) {
    let mut seen: Vec<ColumnType> = Vec::new();
    let mut error: Option<String> = None;
    for row in rows {
        let cell = cell_at(grid, *row, position);
        match (cell, cell_type(cell)) {
            (Cell::Error(shown), _) => {
                error = error.or_else(|| {
                    Some(format!(
                        "{} holds the error {shown}; fix the formula, or leave the row out of the range",
                        cell_name(grid, *row, position)
                    ))
                });
            }
            (_, Some(column_type)) if !seen.contains(&column_type) => seen.push(column_type),
            _ => {}
        }
    }
    let column_type = combined(&seen);
    let warning = (seen.len() > 1 && column_type == ColumnType::Text).then(|| {
        let kinds: Vec<String> = seen.iter().map(ToString::to_string).collect();
        format!(
            "{}: column {name} holds {} values, so it loads as text as the sheet shows it; pin a type in columns to load it as another",
            grid.title,
            kinds.join(" and ")
        )
    });
    let column = SheetColumn {
        name,
        position,
        column_type: error.map_or(Ok(column_type), Err),
        has_values: !seen.is_empty(),
    };
    (column, warning)
}

/// The type a cell suggests: `bool`, `int` for a whole number a sheet holds exactly in a plain format, `float` for other numbers (currency, percentages and scientific numbers always), `date`, `timestamp_ntz` or `time` by its date format, `text` for text (numbers stored as text included); `None` for an empty or error cell.
fn cell_type(cell: &Cell) -> Option<ColumnType> {
    match cell {
        Cell::Empty | Cell::Error(_) => None,
        Cell::Bool(_) => Some(ColumnType::Bool),
        Cell::Text(_) => Some(ColumnType::Text),
        Cell::Number { value, kind, .. } => Some(match kind {
            NumberKind::Date => ColumnType::Date,
            NumberKind::DateTime => ColumnType::TimestampNtz,
            NumberKind::Time if (0.0..1.0).contains(value) => ColumnType::Time,
            NumberKind::Fractional => ColumnType::Float,
            _ if value.trunc() == *value && value.abs() <= MAX_EXACT_INTEGER => ColumnType::Int,
            _ => ColumnType::Float,
        }),
    }
}

/// One type for the types a column's cells suggest: the one they agree on, `float` for whole and other numbers, `timestamp_ntz` for dates and timestamps, `text` for anything else (and for a column without values).
fn combined(seen: &[ColumnType]) -> ColumnType {
    let all = |allowed: &[ColumnType]| seen.iter().all(|seen| allowed.contains(seen));
    match seen {
        [] => ColumnType::Text,
        [only] => only.clone(),
        _ if all(&[ColumnType::Int, ColumnType::Float]) => ColumnType::Float,
        _ if all(&[ColumnType::Date, ColumnType::TimestampNtz]) => ColumnType::TimestampNtz,
        _ => ColumnType::Text,
    }
}
