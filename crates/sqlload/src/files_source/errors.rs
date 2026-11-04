use crate::file_store::errors::StoreError;

/// Why reading files failed.
#[derive(Debug, thiserror::Error)]
pub enum FilesError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{key}: {reason}")]
    File { key: String, reason: String },
}
