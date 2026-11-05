//! Whether an external stage reads a files source's folder.

use crate::store_paths::models::{Location, StoreKind};

/// Whether the external stage on `stage_url` reads the files under `source`: the same S3 bucket and endpoint, below the stage's prefix.
pub(in crate::adapters) fn stage_covers(stage_url: &Location, source: &Location) -> bool {
    match (&source.store, &stage_url.store) {
        (
            StoreKind::S3 {
                bucket, endpoint, ..
            },
            StoreKind::S3 {
                bucket: stage_bucket,
                endpoint: stage_endpoint,
                ..
            },
        ) => {
            bucket == stage_bucket
                && endpoint == stage_endpoint
                && source.prefix.starts_with(&stage_url.prefix)
        }
        _ => false,
    }
}
