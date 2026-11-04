use crate::loader::models::{Destination, TableSpec};
use crate::routing::main::plan_route::plan_route;
use crate::routing::models::{
    FileFormat, FileSet, ReaderOffer, Route, RouteOverrides, Store, WriterAccepts,
};
use crate::snowflake_destination::constants::USER_STAGE;
use crate::store_paths::models::StoreKind;

/// What a destination can take.
fn writer_accepts(destination: &Destination) -> WriterAccepts {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        Destination::Snowflake(_) => {
            let stage = Store::InternalStage(format!("@{USER_STAGE}"));
            WriterAccepts {
                arrow: false,
                pulls_from: vec![stage.clone()],
                pull_formats: vec![FileFormat::Parquet],
                staging: vec![stage],
            }
        }
    }
}

/// What a table's source offers: rows as Arrow, plus the files themselves for a files source.
fn reader_offer(spec: &TableSpec) -> ReaderOffer {
    let existing_files = spec.files.as_ref().map(|files| FileSet {
        store: match files.location.store {
            StoreKind::S3 { .. } => Store::ObjectStore(files.location.to_string()),
            StoreKind::Filesystem { .. } => Store::Local(files.location.to_string()),
        },
        format: FileFormat::Parquet,
    });
    ReaderOffer {
        arrow: true,
        existing_files,
        unload_to: Vec::new(),
        estimated_bytes: match u64::try_from(spec.source_table.estimated_bytes) {
            Ok(bytes) => Some(bytes),
            Err(_) => None,
        },
    }
}

/// The route a table's data takes into `destination`.
pub(crate) fn table_route(spec: &TableSpec, destination: &Destination) -> Result<Route, String> {
    let reader = reader_offer(spec);
    plan_route(
        &reader,
        &writer_accepts(destination),
        RouteOverrides::default(),
    )
    .map_err(|error| error.to_string())
}
