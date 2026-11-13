//! A column's or table's name as dlt's default naming gives it, `__` and all.

use crate::load_naming::_helpers::snake_case::{shortened, snake_case};
use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;

/// `path`, names joined by `__` (a table `orders__LineItems`, or a column `a__B`), as dlt 1.32.0's `snake_case` naming normalises a path, which is how it names columns and tables: each name normalised and shortened to `max_length` on its own, blank ones dropped, joined again, and the whole shortened with a tag of `path`.
pub fn dlt_path(path: &str, max_length: Option<usize>) -> String {
    let names: Vec<String> = path
        .split(CHILD_TABLE_SEPARATOR)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| shortened(snake_case(name), name, max_length))
        .collect();
    shortened(names.join(CHILD_TABLE_SEPARATOR), path, max_length)
}
