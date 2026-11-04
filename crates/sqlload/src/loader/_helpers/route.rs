use crate::loader::models::{Destination, TableSpec};
use crate::routing::main::plan_route::plan_route;
use crate::routing::models::{
    FileFormat, ReaderOffer, Route, RouteOverrides, Store, WriterAccepts,
};
use crate::snowflake_destination::constants::USER_STAGE;

/// What a destination can take.
fn writer_accepts(destination: &Destination) -> WriterAccepts {
    match destination {
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

/// The route a database source's table takes into `destination`.
pub(crate) fn table_route(spec: &TableSpec, destination: &Destination) -> Result<Route, String> {
    let reader = ReaderOffer {
        arrow: true,
        estimated_bytes: match u64::try_from(spec.source_table.estimated_bytes) {
            Ok(bytes) => Some(bytes),
            Err(_) => None,
        },
        ..ReaderOffer::default()
    };
    plan_route(
        &reader,
        &writer_accepts(destination),
        RouteOverrides::default(),
    )
    .map_err(|error| error.to_string())
}
