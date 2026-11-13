//! A child table's name as dlt's default naming gives it.

use crate::load_naming::_helpers::snake_case::shortened;
use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;
use crate::load_naming::main::dlt_identifier::dlt_identifier;

/// The child table of `parent` (a name dlt's naming gave) for its list field `field`, as dlt's `shorten_fragments` joins them: `field` normalised as one name (its `__` merged), joined to `parent` with `__`, and the whole shortened to `max_length` with a tag of the joined name.
pub fn dlt_child_table(parent: &str, field: &str, max_length: Option<usize>) -> String {
    let joined = format!(
        "{parent}{CHILD_TABLE_SEPARATOR}{}",
        dlt_identifier(field, max_length)
    );
    shortened(joined.clone(), &joined, max_length)
}
