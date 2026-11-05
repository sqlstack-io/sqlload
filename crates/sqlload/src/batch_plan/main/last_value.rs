//! The value an ordered read's batch ends with.

use arrow_array::Array;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

/// The last row's value of `values` as [`cut_index`](crate::batch_plan::main::cut_index::cut_index) compares it; `None` for no rows.
pub fn last_value(values: &dyn Array) -> Result<Option<String>, String> {
    if values.is_empty() {
        return Ok(None);
    }
    let formatter = ArrayFormatter::try_new(values, &FormatOptions::default().with_null("\u{0}"))
        .map_err(|error| error.to_string())?;
    Ok(Some(formatter.value(values.len() - 1).to_string()))
}
