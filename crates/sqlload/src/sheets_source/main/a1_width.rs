//! The width of an A1 range of columns.

use crate::sheets_source::_helpers::layout::corner;

/// How many columns `text` spans when it is an A1 range of columns, such as `Prices!A3:C`; `None` for a tab's name alone, which spans the tab's own columns.
pub fn a1_width(text: &str) -> Option<usize> {
    let cells = text.rsplit_once('!').map_or(text, |(_, cells)| cells);
    let (first, last) = cells.split_once(':')?;
    let ((first, _), (last, _)) = (corner(first)?, corner(last)?);
    (last >= first).then(|| (last - first) as usize + 1)
}
