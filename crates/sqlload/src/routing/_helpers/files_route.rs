//! The routes of a source whose data already exists as files.

use crate::routing::_helpers::deliver::deliver_arrow;
use crate::routing::_helpers::stores::destination_pulls;
use crate::routing::errors::RouteError;
use crate::routing::models::{FileSet, ReaderOffer, Route, RouteOverrides, Step, WriterAccepts};

/// Files a warehouse reads itself: in place under a stage, or uploaded unchanged for `copy_to_internal_stage`; never through SQLLoad's reader.
pub(crate) fn warehouse_reads(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    files: &FileSet,
    warehouse: &'static str,
) -> Result<Route, RouteError> {
    let pulls = destination_pulls(writer, &files.store, files.format);
    let copies_to = writer
        .copies_files_to
        .as_ref()
        .filter(|_| reader.copy_to_internal_stage);
    match (pulls, copies_to, &reader.direct_blocked) {
        (_, _, Some(reason)) if pulls || copies_to.is_some() => {
            Err(RouteError::DirectRefused(reason.clone()))
        }
        (true, _, _) => Ok(Route {
            steps: vec![
                Step::UseFiles(files.clone()),
                Step::DestinationPulls(files.clone()),
            ],
            reason: "the files already exist and the destination reads them itself".to_owned(),
        }),
        (false, Some(store), _) => Ok(Route {
            steps: vec![
                Step::UseFiles(files.clone()),
                Step::CopyFiles(store.clone()),
                Step::DestinationPulls(FileSet {
                    store: store.clone(),
                    format: files.format,
                }),
            ],
            reason: format!(
                "copy_to_internal_stage: no external stage covers the files, so they are downloaded and uploaded unchanged, and {warehouse} loads them itself (much slower than an external stage)"
            ),
        }),
        (false, None, _) => Err(RouteError::Unreadable {
            warehouse,
            location: files.store.to_string(),
        }),
    }
}

/// Files for a destination that does not read them itself: in place where it can, otherwise read by SQLLoad.
pub(crate) fn other_reads(
    reader: &ReaderOffer,
    writer: &WriterAccepts,
    files: &FileSet,
    overrides: RouteOverrides,
) -> Result<Route, RouteError> {
    let pulls = destination_pulls(writer, &files.store, files.format);
    match (pulls, &reader.needs_loader, &reader.direct_blocked) {
        (true, None, None) => Ok(Route {
            steps: vec![
                Step::UseFiles(files.clone()),
                Step::DestinationPulls(files.clone()),
            ],
            reason: "the files already exist and the destination reads them itself".to_owned(),
        }),
        (_, Some(reason), _) | (true, None, Some(reason)) => loader_reads(
            files,
            writer,
            overrides,
            &format!("SQLLoad reads the files because {reason}"),
        ),
        (false, None, _) => loader_reads(
            files,
            writer,
            overrides,
            "the destination cannot read the files where they are, so SQLLoad reads them",
        ),
    }
}

/// SQLLoad reads the files into Arrow and hands them over: the only route on which file data passes through SQLLoad.
pub(crate) fn loader_reads(
    files: &FileSet,
    writer: &WriterAccepts,
    overrides: RouteOverrides,
    reason: &str,
) -> Result<Route, RouteError> {
    deliver_arrow(
        vec![
            Step::UseFiles(files.clone()),
            Step::FileReader(files.format),
        ],
        writer,
        overrides.stage,
        reason,
    )
}
