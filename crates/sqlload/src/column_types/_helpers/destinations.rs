use std::collections::BTreeSet;

use crate::column_types::_helpers::names::NameKind;
use crate::column_types::errors::TypeError;
use crate::column_types::models::{ColumnType, Warehouse};

/// Reject a warehouse-specific name when the table also loads into a warehouse that does not use it.
pub(crate) fn check_destinations(
    kind: NameKind,
    column_type: &ColumnType,
    written: &str,
    destinations: &[Warehouse],
) -> Result<(), TypeError> {
    let NameKind::Specific(allowed) = kind else {
        return Ok(());
    };
    let others: BTreeSet<Warehouse> = destinations
        .iter()
        .copied()
        .filter(|destination| !allowed.contains(&destination.to_string().as_str()))
        .collect();
    if others.is_empty() {
        return Ok(());
    }
    Err(TypeError::WarehouseSpecific {
        written: written.to_owned(),
        warehouse: allowed[0],
        others: others
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        portable: column_type.to_string(),
    })
}

/// A warning when a `native` type is passed to more than one kind of warehouse.
pub(crate) fn native_warnings(name: &str, destinations: &[Warehouse]) -> Vec<String> {
    let engines: BTreeSet<Warehouse> = destinations.iter().copied().collect();
    if engines.len() <= 1 {
        return Vec::new();
    }
    vec![format!(
        "native({name:?}) is passed unchanged to {}; check that each of them has this type",
        engines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )]
}
