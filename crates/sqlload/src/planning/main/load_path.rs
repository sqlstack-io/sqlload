//! Choose the delivery path.

use crate::config::models::{Config, Destination, LoadMethod, StagingKind};
use crate::planning::models::LoadPath;

/// How rows reach tables for this configuration; `gcs` on Snowflake, which configuration checks refuse, falls to the `s3` copy.
pub fn load_path(config: &Config, uses_key_pair: bool) -> LoadPath {
    match (&config.destination, config.staging.kind) {
        (Destination::Databricks(_), StagingKind::None) => return LoadPath::Zerobus,
        (Destination::Databricks(_), StagingKind::Internal) => return LoadPath::VolumeCopy,
        (Destination::Databricks(_), _) => return LoadPath::S3Copy,
        (Destination::BigQuery(_), StagingKind::None) => return LoadPath::WriteApi,
        (Destination::BigQuery(_), StagingKind::Internal) => return LoadPath::UploadLoad,
        (Destination::BigQuery(_), _) => return LoadPath::GcsLoad,
        (Destination::Snowflake(_), _) => {}
    }
    match (config.staging.kind, config.staging.load) {
        (StagingKind::None, _) => LoadPath::Streaming,
        (StagingKind::Gcs, _) => LoadPath::S3Copy,
        (StagingKind::Internal, LoadMethod::Copy) => LoadPath::StageCopy,
        (StagingKind::Internal, LoadMethod::Snowpipe) => LoadPath::StageInsertFiles,
        (StagingKind::Internal, LoadMethod::Auto) if uses_key_pair => LoadPath::StageInsertFiles,
        (StagingKind::Internal, LoadMethod::Auto) => LoadPath::StageCopy,
        (StagingKind::S3, LoadMethod::Copy) => LoadPath::S3Copy,
        (StagingKind::S3, _) if config.staging.s3.as_ref().is_some_and(|s3| s3.auto_ingest) => {
            LoadPath::S3AutoIngest
        }
        (StagingKind::S3, LoadMethod::Snowpipe) => LoadPath::S3InsertFiles,
        (StagingKind::S3, LoadMethod::Auto) if uses_key_pair => LoadPath::S3InsertFiles,
        (StagingKind::S3, LoadMethod::Auto) => LoadPath::S3Copy,
    }
}
