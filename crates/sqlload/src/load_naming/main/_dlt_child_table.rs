//! A child table's name as dlt's default naming gives it.

use crate::load_naming::_helpers::snake_case::shortened;
use crate::load_naming::constants::{CHILD_TABLE_SEPARATOR, DLT_EMPTY_NAME};
use crate::load_naming::main::_dlt_path::dlt_path;

/// The child table of `parent` (a name dlt's naming gave) for its list field `field`, as a dlt pipeline names it: `field` normalised as a path (each `__`-separated part on its own, as dlt's relational normaliser gives `shorten_fragments` its keys), or [`DLT_EMPTY_NAME`] when it is blank, joined to `parent` with `__`, and the whole shortened to `max_length` with a tag of the joined name.
pub(in crate::load_naming) fn dlt_child_table(
    parent: &str,
    field: &str,
    max_length: Option<usize>,
) -> String {
    let field = match field.trim().is_empty() {
        true => DLT_EMPTY_NAME.to_owned(),
        false => dlt_path(field, max_length),
    };
    let joined = format!("{parent}{CHILD_TABLE_SEPARATOR}{field}");
    shortened(joined.clone(), &joined, max_length)
}
