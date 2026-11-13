use crate::file_store::errors::StoreError;

/// Why reading files failed.
#[derive(Debug, thiserror::Error)]
pub enum FilesError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{key}: {reason}")]
    File { key: String, reason: String },
    /// The folder `key` lists no file of the source's format, and none that `path_columns` refuses.
    #[error("{key}: {reason}")]
    NoFiles { key: String, reason: String },
}

/// Why a batch could not be shaped for the destination, and the first row (within the batch) at fault when one is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeError {
    pub row: Option<usize>,
    pub reason: String,
}
