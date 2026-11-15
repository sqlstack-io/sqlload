//! The name of a column a load adds.

use crate::load_naming::constants::METADATA_NAMES;
use crate::load_naming::models::{Compat, MetadataRole};

/// The column holding `role` under `compat`.
pub fn metadata_name(compat: Compat, role: MetadataRole) -> &'static str {
    METADATA_NAMES
        .iter()
        .find(|(listed, _, _)| *listed == role)
        .map_or("", |(_, sqlload, dlt)| match compat {
            Compat::Sqlload => sqlload,
            Compat::Dlt => dlt,
        })
}
