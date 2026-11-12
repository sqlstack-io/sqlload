//! The copies a move makes.

use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};

use crate::file_store::constants::MOVE_PARTS_IN_FLIGHT;

/// An `AlreadyExists` error when `to` exists.
pub(crate) async fn refuse_existing(
    store: &dyn ObjectStore,
    to: &Path,
) -> Result<(), object_store::Error> {
    match store.head(to).await {
        Ok(_) => Err(object_store::Error::AlreadyExists {
            path: to.to_string(),
            source: "a file is already there; it is never overwritten".into(),
        }),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Copy `from` through this process into a hidden file beside `to`, then link it to `to` without overwriting; the hidden file is removed either way.
pub(crate) async fn linked_copy(
    store: &dyn ObjectStore,
    from: &Path,
    to: &Path,
) -> Result<(), object_store::Error> {
    let name = to.filename().unwrap_or_default();
    let hidden = Path::from(to.as_ref().strip_suffix(name).unwrap_or_default())
        .join(format!(".{name}.sqlload-{}", uuid::Uuid::new_v4()));
    let linked = match streamed_copy(store, from, &hidden).await {
        Ok(()) => store.copy_if_not_exists(&hidden, to).await,
        Err(error) => Err(error),
    };
    let removed = store.delete(&hidden).await;
    linked?;
    match removed {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Copy `from` to `to` in parts, holding at most [`MOVE_PARTS_IN_FLIGHT`] in memory; a failed copy is aborted.
async fn streamed_copy(
    store: &dyn ObjectStore,
    from: &Path,
    to: &Path,
) -> Result<(), object_store::Error> {
    let mut parts = store.get(from).await?.into_stream();
    let mut upload = WriteMultipart::new(store.put_multipart(to).await?);
    let copied: Result<(), object_store::Error> = async {
        while let Some(part) = parts.next().await {
            upload.wait_for_capacity(MOVE_PARTS_IN_FLIGHT).await?;
            upload.put(part?);
        }
        Ok(())
    }
    .await;
    match copied {
        Ok(()) => upload.finish().await.map(|_| ()),
        Err(error) => {
            let _aborted = upload.abort().await;
            Err(error)
        }
    }
}
