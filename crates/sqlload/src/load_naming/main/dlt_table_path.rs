//! A child table's name as dlt's default naming gives it.

use crate::load_naming::_helpers::snake_case::{shortened, snake_case};
use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;

/// `path`, table names joined by `__` (`orders__LineItems`), as dlt 1.32.0's `snake_case` naming normalises a tables path: each name normalised and shortened to `max_length` on its own, blank ones dropped, joined again, and the whole shortened with a tag of `path`.
pub fn dlt_table_path(path: &str, max_length: Option<usize>) -> String {
    let names: Vec<String> = path
        .split(CHILD_TABLE_SEPARATOR)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| shortened(snake_case(name), name, max_length))
        .collect();
    shortened(names.join(CHILD_TABLE_SEPARATOR), path, max_length)
}
