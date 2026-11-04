//! Read a whole file.

use bytes::Bytes;
use object_store::ObjectStoreExt;

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// The bytes of `relative_key` under `folder`.
pub async fn read_file(folder: &OpenFolder, relative_key: &str) -> Result<Bytes, StoreError> {
    let failed = |source: object_store::Error| StoreError::Object {
        action: "cannot read",
        key: relative_key.to_owned(),
        source,
    };
    let path = object_path(&folder.location.prefix, relative_key);
    let result = folder.store.get(&path).await.map_err(failed)?;
    result.bytes().await.map_err(failed)
}
