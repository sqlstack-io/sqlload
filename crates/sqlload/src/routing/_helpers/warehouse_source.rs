use crate::routing::_helpers::deliver::deliver_arrow;
use crate::routing::_helpers::stores::{destination_pulls, ordered_stores};
use crate::routing::constants::UNLOAD_THRESHOLD_BYTES;
use crate::routing::errors::RouteError;
use crate::routing::models::{
    FileFormat, FileSet, ReadVia, ReaderOffer, Route, RouteOverrides, Step, WriterAccepts,
};

/// Whether a source that can both query and unload should unload.
pub(crate) fn should_unload(reader: &ReaderOffer, read_via: ReadVia) -> Result<bool, RouteError> {
    let can_unload = !reader.unload_to.is_empty();
    match read_via {
        ReadVia::Query if reader.arrow => Ok(false),
        ReadVia::Query => Err(RouteError::ReadViaUnavailable { wanted: "query" }),
        ReadVia::Unload if can_unload => Ok(true),
        ReadVia::Unload => Err(RouteError::ReadViaUnavailable { wanted: "unload" }),
        ReadVia::Auto => Ok(can_unload
            && (!reader.arrow
                || reader
                    .estimated_bytes
                    .is_none_or(|bytes| bytes >= UNLOAD_THRESHOLD_BYTES))),
    }
}

/// Unload the source to Parquet, letting the destination load it directly when it can reach the files.
pub(crate) fn unload_route(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    overrides: RouteOverrides,
) -> Result<Route, RouteError> {
    let stores = ordered_stores(&reader.unload_to, overrides.stage)?;
    if let Some(store) = stores.iter().find(|store| {
        reader.direct_blocked.is_none() && destination_pulls(writer, store, FileFormat::Parquet)
    }) {
        let files = FileSet {
            store: store.clone(),
            format: FileFormat::Parquet,
        };
        return Ok(Route {
            steps: vec![Step::Unload(store.clone()), Step::DestinationPulls(files)],
            reason: format!(
                "unloaded to {store}, which the destination reads itself; no data passes through SQLLoad"
            ),
        });
    }
    let Some(store) = stores.into_iter().next() else {
        return Err(RouteError::NoRoute(
            "the source has nowhere to unload to".to_owned(),
        ));
    };
    deliver_arrow(
        vec![
            Step::Unload(store.clone()),
            Step::FileReader(FileFormat::Parquet),
        ],
        writer,
        overrides.stage,
        &match &reader.direct_blocked {
            Some(reason) => {
                format!("unloaded to {store} and read back by SQLLoad, because {reason}")
            }
            None => format!(
                "unloaded to {store} and read back, because the destination cannot reach it"
            ),
        },
    )
}
