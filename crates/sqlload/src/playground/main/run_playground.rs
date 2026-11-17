//! `sqlload playground`: create a playground project, or add the next change to one.

use std::path::Path;

use crate::playground::_helpers::project::{apply_change, create_playground};
use crate::playground::constants::DEFAULT_NAME;

/// With `change`, change the data of the playground whose sqlload.toml is `config`; else create one in folder `name`.
pub fn run_playground(
    change: bool,
    name: Option<&str>,
    config: &Path,
) -> Result<Vec<String>, String> {
    match change {
        true => apply_change(config),
        false => {
            let name = name.unwrap_or(DEFAULT_NAME);
            create_playground(Path::new(name), name)
        }
    }
}
