//! What a column a load adds holds.

use crate::load_naming::constants::METADATA_NAMES;
use crate::load_naming::models::{Compat, MetadataRole};

/// The role of the column named `name` when it is one the load adds under `compat`, matched exactly; `None` for any other column, including one named like a load's column under the other compat.
pub fn metadata_role(compat: Compat, name: &str) -> Option<MetadataRole> {
    METADATA_NAMES
        .iter()
        .find(|(_, sqlload, dlt)| {
            name == match compat {
                Compat::Sqlload => *sqlload,
                Compat::Dlt => *dlt,
            }
        })
        .map(|(role, _, _)| *role)
}
