//! What a column a load adds holds.

use crate::load_naming::constants::METADATA_NAMES;
use crate::load_naming::models::{MetadataRole, Naming};

/// The role of the column named `name` when it is one the load adds under `naming`, matched exactly; `None` for any other column, including one named like a load's column under the other naming.
pub fn metadata_role(naming: Naming, name: &str) -> Option<MetadataRole> {
    METADATA_NAMES
        .iter()
        .find(|(_, sqlload, dlt)| {
            name == match naming {
                Naming::Sqlload => *sqlload,
                Naming::Dlt => *dlt,
            }
        })
        .map(|(role, _, _)| *role)
}
