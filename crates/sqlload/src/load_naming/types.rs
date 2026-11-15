use std::collections::BTreeMap;

use crate::load_naming::models::RenameColumn;

/// A rename function: the names it gives a table's columns, by position, or why it gives none.
pub type RenameFunction<'a> =
    &'a dyn Fn(&[RenameColumn]) -> Result<BTreeMap<usize, String>, String>;
