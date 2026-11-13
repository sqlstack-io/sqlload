//! One name as dlt's default naming gives it.

use crate::load_naming::_helpers::snake_case::{shortened, snake_case};
use crate::load_naming::constants::DLT_EMPTY_NAME;

/// `name` as dlt 1.32.0's `snake_case` naming normalises an identifier (`OrderID` is `order_id`, `a b` is `a_b`, `1st` is `_1st`), shortened with a tag of the name past `max_length` characters, the destination's limit; [`DLT_EMPTY_NAME`] for a name that is empty or only whitespace, as dlt names such a key.
pub(in crate::load_naming) fn dlt_identifier(name: &str, max_length: Option<usize>) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return DLT_EMPTY_NAME.to_owned();
    }
    shortened(snake_case(trimmed), trimmed, max_length)
}
