//! List the data files under a folder.

use futures_util::TryStreamExt;
use object_store::path::Path;

use crate::file_store::_helpers::keys::is_hidden;
use crate::file_store::errors::StoreError;
use crate::file_store::models::{OpenFolder, StoredFile};

/// Every file below `folder` (recursively), sorted by key, skipping hidden `_`/`.` folders and files.
pub async fn list_files(folder: &OpenFolder) -> Result<Vec<StoredFile>, StoreError> {
    let prefix = folder.location.prefix.clone();
    let listed: Vec<object_store::ObjectMeta> = folder
        .store
        .list(Some(&Path::from(prefix.as_str())))
        .try_collect()
        .await
        .map_err(|source| StoreError::Object {
            action: "cannot list",
            key: folder.location.to_string(),
            source,
        })?;
    let mut files: Vec<StoredFile> = listed
        .into_iter()
        .filter_map(|meta| {
            let key = meta.location.to_string();
            let relative_key = key.strip_prefix(&prefix).unwrap_or(&key).to_owned();
            (!is_hidden(&relative_key)).then(|| StoredFile {
                relative_key,
                size: meta.size,
                etag: meta.e_tag,
                modified_ms: meta.last_modified.timestamp_millis(),
            })
        })
        .collect();
    files.sort_by(|left, right| left.relative_key.cmp(&right.relative_key));
    Ok(files)
}
