//! List the data files under a folder.

use futures_util::TryStreamExt;

use crate::file_store::_helpers::keys::{is_hidden, object_path};
use crate::file_store::errors::StoreError;
use crate::file_store::models::{OpenFolder, StoredFile};

/// Every file below `folder` (recursively), sorted by key, skipping hidden `_`/`.` folders and files.
pub async fn list_files(folder: &OpenFolder) -> Result<Vec<StoredFile>, StoreError> {
    let failed = |source: object_store::Error| StoreError::Object {
        action: "cannot list",
        key: folder.location.to_string(),
        source,
    };
    let prefix = folder.location.prefix.as_str();
    let listed_prefix = match prefix.is_empty() {
        true => None,
        false => Some(object_path(prefix).map_err(failed)?),
    };
    let listed: Vec<object_store::ObjectMeta> = folder
        .store
        .list(listed_prefix.as_ref())
        .try_collect()
        .await
        .map_err(failed)?;
    let mut files: Vec<StoredFile> = Vec::new();
    for meta in listed {
        let Some(relative_key) = meta.location.as_ref().strip_prefix(prefix) else {
            continue;
        };
        if is_hidden(relative_key) {
            continue;
        }
        files.push(StoredFile {
            relative_key: relative_key.to_owned(),
            size: meta.size,
            etag: meta.e_tag,
            modified_ms: meta.last_modified.timestamp_millis(),
            path: meta.location,
        });
    }
    files.sort_by(|left, right| left.relative_key.cmp(&right.relative_key));
    Ok(files)
}
