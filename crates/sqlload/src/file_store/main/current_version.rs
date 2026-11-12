//! Check a file is still the version that was read.

use object_store::ObjectStoreExt;

use crate::file_store::errors::StoreError;
use crate::file_store::models::{OpenFolder, StoredFile};

/// Fail unless `file` is still at the version it was listed at: a file rewritten since then holds rows that were not loaded.
pub async fn current_version(folder: &OpenFolder, file: &StoredFile) -> Result<(), StoreError> {
    let failed = |source: object_store::Error| StoreError::Object {
        action: "cannot check",
        key: file.path.to_string(),
        source,
    };
    let meta = folder.store.head(&file.path).await.map_err(failed)?;
    let now = match &meta.e_tag {
        Some(etag) => etag.trim_matches('"').to_owned(),
        None => format!("{}-{}", meta.size, meta.last_modified.timestamp_millis()),
    };
    match now == file.version() {
        true => Ok(()),
        false => Err(failed(object_store::Error::Generic {
            store: "after_load",
            source: "it changed since it was read, so its new rows are not loaded; it is left where it is".into(),
        })),
    }
}
