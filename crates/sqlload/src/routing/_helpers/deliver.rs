use crate::routing::_helpers::stores::{destination_pulls, ordered_stores};
use crate::routing::errors::RouteError;
use crate::routing::models::{FileFormat, FileSet, Route, StagePreference, Step, WriterAccepts};

/// Finish a route whose data is now Arrow batches: hand them over, or stage Parquet for the destination to load.
pub(crate) fn deliver_arrow(
    mut steps: Vec<Step>,
    writer: &WriterAccepts,
    preference: StagePreference,
    reason: &str,
) -> Result<Route, RouteError> {
    if writer.arrow {
        steps.push(Step::DestinationTakesArrow);
        return Ok(Route {
            steps,
            reason: format!("{reason}; the destination takes Arrow directly"),
        });
    }
    let staging = ordered_stores(&writer.staging, preference)?;
    let Some(store) = staging
        .into_iter()
        .find(|store| destination_pulls(writer, store, FileFormat::Parquet))
    else {
        return Err(RouteError::NoRoute(
            "the destination takes neither Arrow nor staged Parquet files".to_owned(),
        ));
    };
    steps.push(Step::ParquetWriter(store.clone()));
    steps.push(Step::DestinationPulls(FileSet {
        store: store.clone(),
        format: FileFormat::Parquet,
    }));
    Ok(Route {
        steps,
        reason: format!("{reason}; staged as Parquet in {store} for the destination to load"),
    })
}
