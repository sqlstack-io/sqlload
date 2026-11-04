//! Check that a folder can be listed.

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// The number of files and folders directly under `folder`, from one listing request.
pub async fn probe_folder(folder: &OpenFolder) -> Result<usize, StoreError> {
    let failed = |source: object_store::Error| StoreError::Object {
        action: "cannot list",
        key: folder.location.to_string(),
        source,
    };
    let prefix = match folder.location.prefix.is_empty() {
        true => None,
        false => Some(object_path(&folder.location.prefix).map_err(failed)?),
    };
    let listed = folder
        .store
        .list_with_delimiter(prefix.as_ref())
        .await
        .map_err(failed)?;
    Ok(listed.objects.len() + listed.common_prefixes.len())
}
