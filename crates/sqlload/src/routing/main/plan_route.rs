//! Choose how a table's data moves from its source to its destination.

use crate::routing::_helpers::deliver::deliver_arrow;
use crate::routing::_helpers::files_route::{loader_reads, other_reads, warehouse_reads};
use crate::routing::_helpers::warehouse_source::{should_unload, unload_route};
use crate::routing::constants::LOADER_READS_BECAUSE;
use crate::routing::errors::RouteError;
use crate::routing::models::{ReaderOffer, Route, RouteOverrides, Step, WriterAccepts};

/// The cheapest route from `reader` to `writer`, honouring `overrides`: a warehouse source's rows selected by a destination writing through its own connection, unless the table needs the loader.
pub fn plan_route(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    overrides: RouteOverrides,
) -> Result<Route, RouteError> {
    if let Some(files) = &reader.existing_files {
        return match (writer.reads_files_itself, &reader.needs_loader) {
            (Some(warehouse), None) => warehouse_reads(reader, writer, files, warehouse),
            (Some(_), Some(reason)) => loader_reads(
                files,
                writer,
                overrides,
                &format!("{LOADER_READS_BECAUSE} {reason}"),
            ),
            (None, _) => other_reads(reader, writer, files, overrides),
        };
    }
    if reader.selected_by_destination && reader.needs_loader.is_none() {
        return Ok(Route {
            steps: vec![Step::DestinationSelects],
            reason: "the destination writes through the source's own connection, so it selects the rows itself; no data leaves the warehouse".to_owned(),
        });
    }
    if should_unload(reader, overrides.read_via)? {
        return unload_route(reader, writer, overrides);
    }
    if !reader.arrow {
        return Err(RouteError::NoRoute(
            "it neither streams rows nor has files".to_owned(),
        ));
    }
    let reason = match &reader.direct_blocked {
        Some(reason) => format!(
            "the source streams rows, as the destination can't select them itself: {reason}"
        ),
        None => "the source streams rows".to_owned(),
    };
    deliver_arrow(vec![Step::ReadArrow], writer, overrides.stage, &reason)
}
