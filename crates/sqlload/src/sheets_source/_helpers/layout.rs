//! Where a range's header and rows are, and the A1 names of its cells.

use crate::sheets_source::models::{Cell, Grid, HeaderRule};

/// A cell's A1 name in `grid`'s tab (its title quoted unless only letters, digits and `_`), from its row and column within the range (0-based).
pub(crate) fn cell_name(grid: &Grid, row: usize, column: usize) -> String {
    let mut number = grid.start_column as usize + column + 1;
    let mut letters: Vec<char> = Vec::new();
    while number > 0 {
        let digit = (number - 1) % 26;
        letters.push(char::from(b'A' + digit as u8));
        number = (number - 1) / 26;
    }
    let letters: String = letters.into_iter().rev().collect();
    let plain = grid
        .title
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_');
    let tab = match plain {
        true => grid.title.clone(),
        false => quoted_title(&grid.title),
    };
    format!("{tab}!{letters}{}", grid.start_row as usize + row + 1)
}

/// A tab's title as A1 notation quotes it: in single quotes, each quote doubled; alone, the range of the tab's whole grid.
pub(crate) fn quoted_title(title: &str) -> String {
    format!("'{}'", title.replace('\'', "''"))
}

/// The header row's index within the range, if the range has one and reaches it.
pub(crate) fn header_index(grid: &Grid, rule: HeaderRule) -> Option<usize> {
    let index = rule.header_row.saturating_sub(1) as usize;
    (rule.header && index < grid.rows.len()).then_some(index)
}

/// The rows below the header (from the header row when there is none), by index within the range, leaving out rows with nothing in their first `width` cells.
pub(crate) fn data_rows(grid: &Grid, rule: HeaderRule, width: usize) -> Vec<usize> {
    let first = match rule.header {
        true => rule.header_row as usize,
        false => rule.header_row.saturating_sub(1) as usize,
    };
    (first..grid.rows.len())
        .filter(|row| has_value(&grid.rows[*row], width))
        .collect()
}

/// Whether any of a row's first `width` cells holds something.
fn has_value(row: &[Cell], width: usize) -> bool {
    row.iter().take(width).any(|cell| *cell != Cell::Empty)
}

/// The cell at `row` and `column` of the range, empty past the end of its row.
pub(crate) fn cell_at(grid: &Grid, row: usize, column: usize) -> &Cell {
    grid.rows[row].get(column).unwrap_or(&Cell::Empty)
}

/// The text a header cell names its column with, trimmed.
fn header_text(cell: &Cell) -> String {
    match cell {
        Cell::Empty => String::new(),
        Cell::Bool(flag) => if *flag { "TRUE" } else { "FALSE" }.to_owned(),
        Cell::Number { shown, .. } | Cell::Error(shown) => shown.trim().to_owned(),
        Cell::Text(text) => text.trim().to_owned(),
    }
}

/// The range's column names, one per column up to its last named one (`column_1`, … up to its widest row without a header); `Err` names each blank header cell and each name used twice, ignoring case, by cell.
pub(crate) fn header_names(grid: &Grid, rule: HeaderRule) -> Result<Vec<String>, Vec<String>> {
    let Some(index) = header_index(grid, rule) else {
        let width = data_rows(grid, rule, usize::MAX)
            .iter()
            .map(|row| grid.rows[*row].len())
            .max()
            .unwrap_or_default();
        return match rule.header {
            true => Err(vec![format!(
                "{}: the range has no header in its row {}; set header_row, or header = false",
                grid.title, rule.header_row
            )]),
            false if width == 0 => Err(vec![format!(
                "{}: the range has no cells from its row {}; check the range and header_row",
                grid.title, rule.header_row
            )]),
            false => Ok((1..=width)
                .map(|number| format!("column_{number}"))
                .collect()),
        };
    };
    let names: Vec<String> = grid.rows[index].iter().map(header_text).collect();
    let width = names
        .iter()
        .rposition(|name| !name.is_empty())
        .map_or(0, |last| last + 1);
    let mut problems: Vec<String> = Vec::new();
    for (position, name) in names.iter().take(width).enumerate() {
        let earlier = names[..position]
            .iter()
            .any(|other| other.eq_ignore_ascii_case(name));
        match (name.is_empty(), earlier) {
            (true, _) => problems.push(format!(
                "{}: the header cell is blank; name the column, or narrow the range",
                cell_name(grid, index, position)
            )),
            (false, true) => problems.push(format!(
                "{}: the header {name:?} names an earlier column too (ignoring case); rename one",
                cell_name(grid, index, position)
            )),
            (false, false) => {}
        }
    }
    match (problems.is_empty(), width) {
        (true, 0) => Err(vec![format!(
            "{}: the header row {} is empty; set header_row, or header = false",
            grid.title, rule.header_row
        )]),
        (true, _) => Ok(names.into_iter().take(width).collect()),
        (false, _) => Err(problems),
    }
}
