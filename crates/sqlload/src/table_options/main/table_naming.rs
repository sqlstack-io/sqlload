//! How a table names what it loads.

use crate::load_naming::models::Naming;
use crate::project::models::TableSettings;
use crate::table_options::main::layered::layered;

/// How a table names what it loads: its own `naming`, else its source's, else its target's (`target`), else snake_case.
pub fn table_naming(
    (settings, defaults): (&TableSettings, &TableSettings),
    target: Option<Naming>,
) -> Naming {
    layered(&settings.naming, &defaults.naming)
        .map(|(value, _)| value)
        .or(target)
        .unwrap_or_default()
}
