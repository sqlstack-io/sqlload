//! Whose names the columns a load adds take.

use crate::load_naming::models::Compat;
use crate::project::models::TableSettings;
use crate::table_options::main::layered::layered;

/// Whose names the columns a table's load adds take: its own `compat`, else its source's, else its target's (`target`), else SQLLoad's.
pub fn table_compat(
    (settings, defaults): (&TableSettings, &TableSettings),
    target: Option<Compat>,
) -> Compat {
    layered(&settings.compat, &defaults.compat)
        .map(|(value, _)| value)
        .or(target)
        .unwrap_or_default()
}
