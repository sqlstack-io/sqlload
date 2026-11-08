//! Delete files.

use futures_util::{StreamExt, TryStreamExt};

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// Delete each of `relative_keys` under `folder`, in bulk where the store allows (one S3 `DeleteObjects` request per 1,000 keys); a file already gone is not an error.
pub async fn delete_files(folder: &OpenFolder, relative_keys: &[String]) -> Result<(), StoreError> {
    let failed = |key: String, source: object_store::Error| StoreError::Object {
        action: "cannot delete",
        key,
        source,
    };
    let paths = relative_keys
        .iter()
        .map(|relative_key| {
            object_path(&format!("{}{relative_key}", folder.location.prefix))
                .map_err(|error| failed(relative_key.clone(), error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    folder
        .store
        .delete_stream(futures_util::stream::iter(paths.into_iter().map(Ok)).boxed())
        .map(|deleted| match deleted {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error),
        })
        .try_collect::<()>()
        .await
        .map_err(|error| failed(folder.location.to_string(), error))
}
