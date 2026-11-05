//! Where an ordered read may end a commit.

use arrow_array::Array;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

/// The first row of `values` (a cursor column read in order) whose value differs from `last`, the previous batch's last value; `values.len()` when every row has it. A commit ends only there, so no cursor value spans two commits.
pub fn cut_index(values: &dyn Array, last: Option<&str>) -> Result<usize, String> {
    let formatter = ArrayFormatter::try_new(values, &FormatOptions::default().with_null("\u{0}"))
        .map_err(|error| error.to_string())?;
    let Some(last) = last else {
        return Ok(0);
    };
    Ok((0..values.len())
        .find(|index| formatter.value(*index).to_string() != last)
        .unwrap_or(values.len()))
}
