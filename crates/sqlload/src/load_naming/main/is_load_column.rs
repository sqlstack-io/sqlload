//! Whether a column is one a load adds.

use crate::load_naming::constants::SQLLOAD_PREFIX;
use crate::load_naming::main::metadata_role::metadata_role;
use crate::load_naming::models::Compat;

/// Whether `name` is a column a load adds under `compat`: any `_sqlload_*` column, or one of the load's columns under `compat`'s names.
pub fn is_load_column(compat: Compat, name: &str) -> bool {
    name.starts_with(SQLLOAD_PREFIX) || metadata_role(compat, name).is_some()
}
