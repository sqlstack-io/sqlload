//! Open the store a location is in.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

use crate::file_store::errors::StoreError;
use crate::file_store::main::s3_builder::s3_builder;
use crate::file_store::models::OpenFolder;
use crate::http::main::install_tls_provider::install_tls_provider;
use crate::store_paths::models::{Location, StoreKind};

/// A client for `location`'s store; S3 credentials come from `AWS_*` variables, web identity or the instance/container role.
pub fn open_folder(location: &Location) -> Result<OpenFolder, StoreError> {
    let failed = |source: object_store::Error| StoreError::Open {
        location: location.to_string(),
        source,
    };
    install_tls_provider();
    let store: Arc<dyn ObjectStore> = match &location.store {
        StoreKind::S3 {
            bucket,
            region,
            endpoint,
        } => Arc::new(
            s3_builder(bucket, region.as_deref(), endpoint.as_deref())
                .build()
                .map_err(failed)?,
        ),
        StoreKind::Filesystem { root } => {
            std::fs::create_dir_all(root).map_err(|error| {
                failed(object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(error),
                })
            })?;
            Arc::new(LocalFileSystem::new_with_prefix(root).map_err(failed)?)
        }
    };
    Ok(OpenFolder {
        store,
        location: location.clone(),
    })
}
