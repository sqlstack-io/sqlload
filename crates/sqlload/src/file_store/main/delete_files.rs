//! Delete files.

use futures_util::StreamExt;

use crate::file_store::_helpers::keys::object_path;
use crate::file_store::errors::StoreError;
use crate::file_store::models::OpenFolder;

/// Delete each of `relative_keys` under `folder`, in bulk where the store allows; a file already gone is not an error.
pub async fn delete_files(folder: &OpenFolder, relative_keys: &[String]) -> Result<(), StoreError> {
    let failed = |source: object_store::Error| StoreError::Object {
        action: "cannot delete files in",
        key: folder.location.to_string(),
        source,
    };
    let paths = relative_keys
        .iter()
        .map(|relative_key| object_path(&format!("{}{relative_key}", folder.location.prefix)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(failed)?;
    let requested = paths.len();
    let mut deleted = folder
        .store
        .delete_stream(futures_util::stream::iter(paths.into_iter().map(Ok)).boxed());
    let mut settled = 0;
    while let Some(outcome) = deleted.next().await {
        match outcome {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => settled += 1,
            Err(error) => return Err(failed(error)),
        }
    }
    // A bulk request that fails as a whole yields one error for all its keys, so a missing answer means some files may remain.
    match settled == requested {
        true => Ok(()),
        false => Err(failed(object_store::Error::Generic {
            store: "delete",
            source: format!(
                "the store settled {settled} of {requested} deletes; a bulk delete failed as a whole (an S3-compatible store without bulk delete needs AWS_DISABLE_BULK_DELETE=true)"
            )
            .into(),
        })),
    }
}
