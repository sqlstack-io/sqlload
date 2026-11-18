//! What Databricks takes.

use crate::adapters::databricks::constants::OUTSIDE_LOCATIONS;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::routing::models::{FileFormat, Store, WriterAccepts};

/// Parquet files in the staging volume, which SQLLoad writes and `COPY INTO` reads; and Parquet files in S3 under one of its external locations, which `COPY INTO` reads where they lie. Parquet files in S3 no external location covers are refused, never read by SQLLoad.
pub(in crate::adapters) fn writer_accepts(destination: &DatabricksDestination) -> WriterAccepts {
    let volume = Store::InternalStage(destination.volume.clone());
    let locations = destination
        .external_locations
        .get()
        .into_iter()
        .flatten()
        .map(|url| Store::ObjectStore(url.clone()));
    WriterAccepts {
        arrow: false,
        pulls_from: std::iter::once(volume.clone()).chain(locations).collect(),
        pull_formats: vec![FileFormat::Parquet],
        staging: vec![volume],
        must_pull: Some((
            Store::ObjectStore("s3://".to_owned()),
            OUTSIDE_LOCATIONS.to_owned(),
        )),
        ..WriterAccepts::default()
    }
}
