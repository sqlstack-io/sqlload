use std::sync::Arc;

use object_store::ObjectStore;

use crate::store_paths::models::Location;

/// An opened folder: the store client and the folder within it.
#[derive(Debug, Clone)]
pub struct OpenFolder {
    pub store: Arc<dyn ObjectStore>,
    pub location: Location,
}

/// One data file under a folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    /// The key relative to the folder, such as `date=2026-11-04/part-0.parquet`.
    pub relative_key: String,
    pub size: u64,
    /// The store's version tag, used to notice a file rewritten in place.
    pub etag: Option<String>,
    pub modified_ms: i64,
}
