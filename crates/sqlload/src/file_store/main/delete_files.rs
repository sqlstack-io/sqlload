//! Delete files.

use object_store::ObjectStoreExt;

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// Delete each of `relative_keys` under `folder`; a file already gone is not an error.
pub async fn delete_files(folder: &OpenFolder, relative_keys: &[String]) -> Result<(), StoreError> {
    for relative_key in relative_keys {
        let failed = |source: object_store::Error| StoreError::Object {
            action: "cannot delete",
            key: relative_key.clone(),
            source,
        };
        let path =
            object_path(&format!("{}{relative_key}", folder.location.prefix)).map_err(failed)?;
        match folder.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(failed(error)),
        }
    }
    Ok(())
}
