//! What Databricks takes.

use crate::adapters::databricks::models::DatabricksDestination;
use crate::routing::models::{FileFormat, Store, WriterAccepts};

/// Parquet files in the staging volume, which SQLLoad writes and `COPY INTO` reads.
pub(in crate::adapters) fn writer_accepts(destination: &DatabricksDestination) -> WriterAccepts {
    let volume = Store::InternalStage(destination.volume.clone());
    WriterAccepts {
        arrow: false,
        pulls_from: vec![volume.clone()],
        pull_formats: vec![FileFormat::Parquet],
        staging: vec![volume],
        ..WriterAccepts::default()
    }
}
