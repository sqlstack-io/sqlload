use crate::file_store::errors::StoreError;

/// Why a table's lease could not be read or written.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{key}: {reason}")]
    Unreadable { key: String, reason: String },
}
