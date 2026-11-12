//! The name of a child table.

use crate::load_naming::constants::CHILD_TABLE_SEPARATOR;
use crate::load_naming::models::Naming;

/// The table the items of `parent`'s list field `field` land in under `naming`: `parent__field`, and `parent__field__child` a level down.
pub fn child_table_name(_naming: Naming, parent: &str, field: &str) -> String {
    format!("{parent}{CHILD_TABLE_SEPARATOR}{field}")
}
