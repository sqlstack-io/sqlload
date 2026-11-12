//! Move a file within a store, never overwriting one, and only the version that was read.

use object_store::ObjectStoreExt;

use crate::file_store::_helpers::copies::{linked_copy, refuse_existing};
use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::main::current_version::current_version;
use crate::file_store::models::{OpenFolder, StoredFile};

/// Move `file` to the store key `to`, which must not exist, once its version is still the one read holds: a copy that refuses to overwrite (a hard link on a local disk); across local volumes, a copy streamed into a hidden file beside `to`, then linked to `to` the same way; on S3, which has no such copy, a server-side copy once `to` is found free, so only a file written to `to` at that moment could be replaced. The source is deleted once the copy has the size that was read.
pub async fn move_file(folder: &OpenFolder, file: &StoredFile, to: &str) -> Result<(), StoreError> {
    let failed = |source_error: object_store::Error| StoreError::Object {
        action: "cannot move",
        key: format!("{} to {to}", file.path),
        source: source_error,
    };
    current_version(folder, file).await?;
    let target = object_path(to).map_err(failed)?;
    let store = folder.store.as_ref();
    match store.copy_if_not_exists(&file.path, &target).await {
        Ok(()) => {}
        Err(error @ object_store::Error::AlreadyExists { .. }) => return Err(failed(error)),
        Err(object_store::Error::NotSupported { .. }) => {
            refuse_existing(store, &target).await.map_err(failed)?;
            store.copy(&file.path, &target).await.map_err(failed)?;
        }
        Err(_) => linked_copy(store, &file.path, &target)
            .await
            .map_err(failed)?,
    }
    let copied = store.head(&target).await.map_err(failed)?.size;
    if copied != file.size {
        return Err(failed(object_store::Error::Generic {
            store: "move",
            source: format!(
                "the copy holds {copied} bytes of the {} read; the source is kept",
                file.size
            )
            .into(),
        }));
    }
    store.delete(&file.path).await.map_err(failed)
}
