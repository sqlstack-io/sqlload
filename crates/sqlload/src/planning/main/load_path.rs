//! Choose the delivery path.

use crate::config::models::{Config, LoadMethod, StagingKind};
use crate::planning::models::LoadPath;

/// How rows reach tables for this configuration.
pub fn load_path(config: &Config, uses_key_pair: bool) -> LoadPath {
    match (config.staging.kind, config.staging.load) {
        (StagingKind::None, _) => LoadPath::Streaming,
        (StagingKind::Internal, LoadMethod::Copy) => LoadPath::StageCopy,
        (StagingKind::Internal, LoadMethod::Snowpipe) => LoadPath::StageInsertFiles,
        (StagingKind::Internal, LoadMethod::Auto) if uses_key_pair => LoadPath::StageInsertFiles,
        (StagingKind::Internal, LoadMethod::Auto) => LoadPath::StageCopy,
        (StagingKind::S3, LoadMethod::Copy) => LoadPath::S3Copy,
        (StagingKind::S3, _) => match config.staging.s3.as_ref().is_some_and(|s3| s3.auto_ingest) {
            true => LoadPath::S3AutoIngest,
            false => LoadPath::S3Copy,
        },
    }
}
