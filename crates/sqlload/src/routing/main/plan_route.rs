//! Choose how a table's data moves from its source to its destination.

use crate::routing::_helpers::deliver::deliver_arrow;
use crate::routing::_helpers::stores::destination_pulls;
use crate::routing::_helpers::warehouse_source::{should_unload, unload_route};
use crate::routing::errors::RouteError;
use crate::routing::models::{FileSet, ReaderOffer, Route, RouteOverrides, Step, WriterAccepts};

/// The cheapest route from `reader` to `writer`, honouring `overrides`.
pub fn plan_route(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    overrides: RouteOverrides,
) -> Result<Route, RouteError> {
    if let Some(files) = &reader.existing_files {
        let pulls = destination_pulls(writer, &files.store, files.format);
        match (&reader.needs_loader, writer.reads_files_itself, pulls) {
            (None, Some(warehouse), false) => {
                let (Some(store), true) = (&writer.copies_files_to, reader.copy_to_internal_stage)
                else {
                    return Err(RouteError::Unreadable {
                        warehouse,
                        location: files.store.to_string(),
                    });
                };
                if let Some(reason) = &reader.direct_blocked {
                    return Err(RouteError::DirectRefused(reason.clone()));
                }
                let copied = FileSet {
                    store: store.clone(),
                    format: files.format,
                };
                return Ok(Route {
                    steps: vec![
                        Step::UseFiles(files.clone()),
                        Step::CopyFiles(store.clone()),
                        Step::DestinationPulls(copied),
                    ],
                    reason: format!(
                        "copy_to_internal_stage: no external stage covers the files, so they are downloaded and uploaded unchanged, and {warehouse} loads them itself (much slower than an external stage)"
                    ),
                });
            }
            (None, Some(_), true) => {
                if let Some(reason) = &reader.direct_blocked {
                    return Err(RouteError::DirectRefused(reason.clone()));
                }
            }
            _ => {}
        }
        if pulls && reader.needs_loader.is_none() && reader.direct_blocked.is_none() {
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
            &match (&reader.needs_loader, &reader.direct_blocked, pulls) {
                (Some(reason), _, _) | (None, Some(reason), true) => {
                    format!("SQLLoad reads the files because {reason}")
                }
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
