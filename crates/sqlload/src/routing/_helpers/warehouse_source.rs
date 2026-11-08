use crate::routing::_helpers::deliver::deliver_arrow;
use crate::routing::_helpers::stores::{destination_pulls, ordered_stores};
use crate::routing::constants::{DOWNLOAD_FOLDER, UNLOAD_THRESHOLD_BYTES};
use crate::routing::errors::RouteError;
use crate::routing::models::{
    FileFormat, FileSet, ReadVia, ReaderOffer, Route, RouteOverrides, Step, Store, WriterAccepts,
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

/// Unload the source to Parquet, letting the destination load it directly when it can reach the files, or the files downloaded unchanged from an internal stage, and nothing needs SQLLoad to read them (masking, checks).
pub(crate) fn unload_route(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    overrides: RouteOverrides,
) -> Result<Route, RouteError> {
    let stores = ordered_stores(&reader.unload_to, overrides.stage)?;
    let unblocked = reader.direct_blocked.is_none() && reader.needs_loader.is_none();
    if let Some(store) = stores
        .iter()
        .find(|store| unblocked && destination_pulls(writer, store, FileFormat::Parquet))
    {
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
    let downloaded = FileSet {
        store: Store::Local(DOWNLOAD_FOLDER.to_owned()),
        format: FileFormat::Parquet,
    };
    if let Some(store) = stores.iter().find(|store| {
        unblocked
            && matches!(store, Store::InternalStage(_))
            && destination_pulls(writer, &downloaded.store, downloaded.format)
    }) {
        return Ok(Route {
            steps: vec![
                Step::Unload(store.clone()),
                Step::CopyFiles(downloaded.store.clone()),
                Step::DestinationPulls(downloaded),
            ],
            reason: format!(
                "unloaded to {store}, which the destination cannot reach, so SQLLoad downloads the files unchanged to {DOWNLOAD_FOLDER}, which the destination reads itself"
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
        &match reader
            .direct_blocked
            .as_ref()
            .or(reader.needs_loader.as_ref())
        {
            Some(reason) => {
                format!("unloaded to {store} and read back by SQLLoad, because {reason}")
            }
            None => format!(
                "unloaded to {store} and read back, because the destination cannot reach it"
            ),
        },
    )
}
