use crate::file_store::errors::StoreError;
use crate::http::errors::ApiError;

/// Why reading from Snowflake failed.
#[derive(Debug, thiserror::Error)]
pub enum SnowflakeSourceError {
    #[error("Snowflake: {0}")]
    Api(#[from] ApiError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{0}")]
    Invalid(String),
    #[error("cannot read unloaded file {file}: {reason}")]
    File { file: String, reason: String },
    /// What the caller's batch consumer failed with, as it said it.
    #[error("{0}")]
    Consumer(String),
}
