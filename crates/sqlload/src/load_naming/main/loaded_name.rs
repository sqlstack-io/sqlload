//! The name a source's table or column loads under.

use crate::load_naming::main::_dlt_path::dlt_path;
use crate::load_naming::models::Naming;

/// `name`, a source's table or column name, as it loads under `naming`: as it is under SQLLoad's naming, and as dlt's naming normalises it, shortened past `max_length`, under dlt's.
pub fn loaded_name(naming: Naming, name: &str, max_length: Option<usize>) -> String {
    match naming {
        Naming::Sqlload => name.to_owned(),
        Naming::Dlt => dlt_path(name, max_length),
    }
}
