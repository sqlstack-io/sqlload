//! The name of a column a load adds.

use crate::load_naming::constants::METADATA_NAMES;
use crate::load_naming::models::{MetadataRole, Naming};

/// The column holding `role` under `naming`.
pub fn metadata_name(naming: Naming, role: MetadataRole) -> &'static str {
    METADATA_NAMES
        .iter()
        .find(|(listed, _, _)| *listed == role)
        .map_or("", |(_, sqlload, dlt)| match naming {
            Naming::Sqlload => sqlload,
            Naming::Dlt => dlt,
        })
}
