//! A child table's name as dlt's default naming gives it.

use crate::load_naming::_helpers::snake_case::shortened;
use crate::load_naming::constants::{CHILD_TABLE_SEPARATOR, DLT_EMPTY_NAME};
use crate::load_naming::main::_dlt_path::dlt_path;

/// The child table of `parent` (a name dlt's naming gave) for its list field `field`, as a dlt pipeline names it: `field` normalised as a path (each `__`-separated part on its own, as dlt's relational normaliser gives `shorten_fragments` its keys), or [`DLT_EMPTY_NAME`] when that leaves nothing (a blank field, or one of only `__`), joined to `parent` with `__`, and the whole shortened to `max_length` with a tag of the joined name.
pub(in crate::load_naming) fn dlt_child_table(
    parent: &str,
    field: &str,
    max_length: Option<usize>,
) -> String {
    let field = match dlt_path(field, max_length) {
        path if path.is_empty() => DLT_EMPTY_NAME.to_owned(),
        path => path,
    };
    let joined = format!("{parent}{CHILD_TABLE_SEPARATOR}{field}");
    shortened(joined.clone(), &joined, max_length)
}
