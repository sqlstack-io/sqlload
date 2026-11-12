//! Move a file within a store, never overwriting one.

use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// Move `relative_key` under `folder` to the store key `to`, which must not exist: a copy that refuses to overwrite where the store has one (a hard link on a local disk), otherwise, as across volumes or on S3, a streamed copy after checking `to` is free; the source is deleted only once the copy has the source's size.
pub async fn move_file(
    folder: &OpenFolder,
    relative_key: &str,
    to: &str,
) -> Result<(), StoreError> {
    let source = format!("{}{relative_key}", folder.location.prefix);
    let failed = |source_error: object_store::Error| StoreError::Object {
        action: "cannot move",
        key: format!("{source} to {to}"),
        source: source_error,
    };
    let from = object_path(&source).map_err(failed)?;
    let target = object_path(to).map_err(failed)?;
    let store = folder.store.as_ref();
    let size = store.head(&from).await.map_err(failed)?.size;
    match store.copy_if_not_exists(&from, &target).await {
        Ok(()) => {}
        Err(error @ object_store::Error::AlreadyExists { .. }) => return Err(failed(error)),
        Err(_) => streamed_copy(store, (&from, &target))
            .await
            .map_err(failed)?,
    }
    let copied = store.head(&target).await.map_err(failed)?.size;
    if copied != size {
        return Err(failed(object_store::Error::Generic {
            store: "move",
            source: format!("the copy holds {copied} bytes of {size}; the source is kept").into(),
        }));
    }
    store.delete(&from).await.map_err(failed)
}

/// Copy `from` to `to` through this process, in parts, after checking `to` does not exist.
async fn streamed_copy(
    store: &dyn ObjectStore,
    (from, to): (&object_store::path::Path, &object_store::path::Path),
) -> Result<(), object_store::Error> {
    match store.head(to).await {
        Ok(_) => {
            return Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "a file is already there; it is never overwritten".into(),
            });
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    let mut parts = store.get(from).await?.into_stream();
    let mut upload = WriteMultipart::new(store.put_multipart(to).await?);
    while let Some(part) = parts.next().await {
        upload.put(part?);
    }
    upload.finish().await.map(|_| ())
}
