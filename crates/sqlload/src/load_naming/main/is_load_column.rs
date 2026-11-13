//! Whether a column is one a load adds.

use crate::load_naming::constants::SQLLOAD_PREFIX;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::Naming;

/// Whether `name` is a column a load adds under `naming`: any `_sqlload_*` column, or one of the load's columns under `naming`'s names.
pub fn is_load_column(naming: Naming, name: &str) -> bool {
    name.starts_with(SQLLOAD_PREFIX) || metadata_role(naming, name).is_some()
}
