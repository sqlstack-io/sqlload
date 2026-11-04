//! Write a whole file so readers see it only once complete.

use bytes::Bytes;
use object_store::{ObjectStoreExt, PutPayload};

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// Write `body` to `relative_key` under `folder` in one put (S3 and local files appear atomically).
pub async fn write_file(
    folder: &OpenFolder,
    relative_key: &str,
    body: Bytes,
) -> Result<(), StoreError> {
    let path = object_path(&folder.location.prefix, relative_key);
    folder
        .store
        .put(&path, PutPayload::from(body))
        .await
        .map(|_| ())
        .map_err(|source| StoreError::Object {
            action: "cannot write",
            key: relative_key.to_owned(),
            source,
        })
}
