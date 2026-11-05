//! Choose how a table's data moves from its source to its destination.

use crate::routing::_helpers::deliver::deliver_arrow;
use crate::routing::_helpers::stores::destination_pulls;
use crate::routing::_helpers::warehouse_source::{should_unload, unload_route};
use crate::routing::errors::RouteError;
use crate::routing::models::{ReaderOffer, Route, RouteOverrides, Step, WriterAccepts};

/// The cheapest route from `reader` to `writer`, honouring `overrides`.
pub fn plan_route(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    overrides: RouteOverrides,
) -> Result<Route, RouteError> {
    if let Some(files) = &reader.existing_files {
        let pulls = destination_pulls(writer, &files.store, files.format);
        if pulls && reader.direct_blocked.is_none() {
            return Ok(Route {
                steps: vec![
                    Step::UseFiles(files.clone()),
                    Step::DestinationPulls(files.clone()),
                ],
                reason: "the files already exist and the destination reads them itself".to_owned(),
            });
        }
        return deliver_arrow(
            vec![
                Step::UseFiles(files.clone()),
                Step::FileReader(files.format),
            ],
            writer,
            overrides.stage,
            &match (&reader.direct_blocked, pulls) {
                (Some(reason), true) => format!("SQLLoad reads the files because {reason}"),
                _ => "the destination cannot read the files where they are, so SQLLoad reads them"
                    .to_owned(),
            },
        );
    }
    if should_unload(reader, overrides.read_via)? {
        return unload_route(reader, writer, overrides);
    }
    if !reader.arrow {
        return Err(RouteError::NoRoute(
            "it neither streams rows nor has files".to_owned(),
        ));
    }
    deliver_arrow(
        vec![Step::ReadArrow],
        writer,
        overrides.stage,
        "the source streams rows",
    )
}
