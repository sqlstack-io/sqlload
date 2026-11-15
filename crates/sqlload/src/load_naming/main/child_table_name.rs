//! The name of a child table.

use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;
use crate::load_naming::main::_dlt_child_table::dlt_child_table;
use crate::load_naming::models::Naming;

/// The table the items of `parent`'s list field `field` land in under `naming`: `parent__field`, and `parent__field__child` a level down; under snake_case with `field` normalised and the whole shortened past `max_length`, the destination's limit, as dlt names it.
pub fn child_table_name(
    (naming, max_length): (Naming, Option<usize>),
    parent: &str,
    field: &str,
) -> String {
    match naming {
        Naming::SnakeCase => dlt_child_table(parent, field, max_length),
        Naming::Source => format!("{parent}{CHILD_TABLE_SEPARATOR}{field}"),
    }
}
