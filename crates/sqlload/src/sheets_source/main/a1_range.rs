//! A1 ranges such as `Prices!A3:C` and `'Q4 Report'!B3:H10`, read as Google Sheets reads them.

use crate::sheets_source::_helpers::layout::corner;
use crate::sheets_source::models::A1Range;

/// `text` as an A1 range: an optional sheet (in single quotes, each quote doubled, when it has other characters than letters, digits and `_`), then `!`, then two corners joined by `:`, each a column's letters with an optional row number. A row left out of the first corner is row 1, and of the last, the sheet's last row.
pub fn a1_range(text: &str) -> Result<A1Range, String> {
    let wrong = |why: &str| {
        format!(
            "range {text:?} {why}; write columns and optional rows, after an optional sheet, as in \"A:C\", \"Prices!A3:C\" or \"'Q4 Report'!B3:H10\""
        )
    };
    let (sheet, cells) = match text.strip_prefix('\'') {
        Some(quoted) => {
            let mut name = String::new();
            let mut characters = quoted.char_indices();
            let rest = loop {
                match characters.next() {
                    Some((at, '\'')) if quoted[at + 1..].starts_with('\'') => {
                        name.push('\'');
                        characters.next();
                    }
                    Some((at, '\'')) => break &quoted[at + 1..],
                    Some((_, character)) => name.push(character),
                    None => return Err(wrong("has a sheet name without its closing quote")),
                }
            };
            let cells = rest
                .strip_prefix('!')
                .ok_or_else(|| wrong("needs a ! after its quoted sheet name"))?;
            (Some(name), cells)
        }
        None => match text.rsplit_once('!') {
            Some((name, cells)) => (Some(name.to_owned()), cells),
            None => (None, text),
        },
    };
    if sheet.as_deref().is_some_and(str::is_empty) {
        return Err(wrong("has an empty sheet name"));
    }
    let (first, last) = cells
        .split_once(':')
        .ok_or_else(|| wrong("needs two corners joined by :"))?;
    let (first_column, first_row) = corner(first)
        .ok_or_else(|| wrong("has a first corner that is not a column and an optional row"))?;
    let (last_column, last_row) = corner(last)
        .ok_or_else(|| wrong("has a last corner that is not a column and an optional row"))?;
    if last_column < first_column {
        return Err(wrong("ends at a column left of where it starts"));
    }
    let first_row = first_row.unwrap_or(1);
    if last_row.is_some_and(|last_row| last_row < first_row) {
        return Err(wrong("ends at a row above where it starts"));
    }
    Ok(A1Range {
        sheet,
        first_column,
        last_column,
        first_row,
        last_row,
    })
}
