//! The name a source's table or column loads under.

use crate::load_naming::main::_dlt_path::dlt_path;
use crate::load_naming::models::Naming;

/// `name`, a source's table or column name, as it loads under `naming`: in dlt's snake_case, shortened past `max_length`, or as the source spells it.
pub fn loaded_name(naming: Naming, name: &str, max_length: Option<usize>) -> String {
    match naming {
        Naming::SnakeCase => dlt_path(name, max_length),
        Naming::Source => name.to_owned(),
    }
}
