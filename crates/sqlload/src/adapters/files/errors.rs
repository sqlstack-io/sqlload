use crate::file_store::errors::StoreError;

/// Why writing a files destination failed.
#[derive(Debug, thiserror::Error)]
pub enum FilesDestinationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{key}: {reason}")]
    Write { key: String, reason: String },
}
