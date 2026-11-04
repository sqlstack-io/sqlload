use crate::routing::errors::RouteError;
use crate::routing::models::{FileFormat, StagePreference, Store, WriterAccepts};

/// `candidates` allowed by `preference`, external stores first.
pub(crate) fn ordered_stores(
    candidates: &[Store],
    preference: StagePreference,
) -> Result<Vec<Store>, RouteError> {
    let external = candidates
        .iter()
        .filter(|store| !matches!(store, Store::InternalStage(_)));
    let internal = candidates
        .iter()
        .filter(|store| matches!(store, Store::InternalStage(_)));
    let ordered: Vec<Store> = match preference {
        StagePreference::Auto => external.chain(internal).cloned().collect(),
        StagePreference::External => external.cloned().collect(),
        StagePreference::Internal => internal.cloned().collect(),
    };
    match (ordered.is_empty(), preference) {
        (true, StagePreference::External) => {
            Err(RouteError::StageUnavailable { wanted: "external" })
        }
        (true, StagePreference::Internal) => {
            Err(RouteError::StageUnavailable { wanted: "internal" })
        }
        _ => Ok(ordered),
    }
}

/// Whether the destination can read `format` files in `store` itself.
pub(crate) fn destination_pulls(writer: &WriterAccepts, store: &Store, format: FileFormat) -> bool {
    writer.pull_formats.contains(&format)
        && writer
            .pulls_from
            .iter()
            .any(|reachable| store.is_within(reachable))
}
