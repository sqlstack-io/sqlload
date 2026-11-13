//! A child row's id.

use crate::row_ids::_helpers::digest::digest_row_id;

/// The id of the item at `index` (from 0) of a list in the row `parent_id`, landing in the child table `table`: the same on every load of the same parent, as dlt's `get_nested_row_hash`.
pub fn child_row_id(parent_id: &str, table: &str, index: usize) -> String {
    digest_row_id(format!("{parent_id}_{table}_{index}").as_bytes())
}
