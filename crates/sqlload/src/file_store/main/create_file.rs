//! Write a file only if it does not exist yet.

use bytes::Bytes;
use object_store::{PutMode, PutOptions, PutPayload};

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// Write `body` to `relative_key` under `folder` unless a file is already there (`If-None-Match: *` on S3, a hard link on a local folder): whether this call created it, so of two concurrent callers for one key exactly one gets `true`.
pub async fn create_file(
    folder: &OpenFolder,
    relative_key: &str,
    body: Bytes,
) -> Result<bool, StoreError> {
    let failed = |source: object_store::Error| StoreError::Object {
        action: "cannot create",
        key: relative_key.to_owned(),
        source,
    };
    let path = object_path(&format!("{}{relative_key}", folder.location.prefix)).map_err(failed)?;
    let options = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match folder
        .store
        .put_opts(&path, PutPayload::from(body), options)
        .await
    {
        Ok(_) => Ok(true),
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => Ok(false),
        Err(error) => Err(failed(error)),
    }
}
