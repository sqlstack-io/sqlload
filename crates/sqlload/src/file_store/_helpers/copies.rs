//! The copies a move makes where the store has no copy that refuses to overwrite.

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::store_paths::main::local_store_root::local_store_root;

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

/// Copy `from` to `to` under the local folder `root` into a file created only if none is there, as where a hard link fails (across volumes, or on a filesystem without them); a failed copy removes the file it created.
pub(crate) async fn exclusive_copy(
    root: &str,
    from: &Path,
    to: &Path,
) -> Result<(), object_store::Error> {
    let local = LocalFileSystem::new_with_prefix(local_store_root(root).as_ref())?;
    let (source, target) = (
        local.path_to_filesystem(from)?,
        local.path_to_filesystem(to)?,
    );
    let shown = to.to_string();
    tokio::task::spawn_blocking(move || {
        let parent = target.parent().unwrap_or(&target);
        std::fs::create_dir_all(parent)?;
        let mut created = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)?;
        let copied = std::fs::File::open(&source)
            .and_then(|mut read| std::io::copy(&mut read, &mut created))
            .and_then(|_| created.sync_all());
        copied.inspect_err(|_| {
            let _removed = std::fs::remove_file(&target);
        })
    })
    .await
    .map_err(|error| object_store::Error::Generic {
        store: "LocalFileSystem",
        source: Box::new(error),
    })?
    .map_err(|error| match error.kind() {
        std::io::ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
            path: shown,
            source: "a file is already there; it is never overwritten".into(),
        },
        _ => object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(error),
        },
    })
}
