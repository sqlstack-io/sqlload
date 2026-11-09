use crate::http::errors::ApiError;

/// Why reading a BigQuery source failed.
#[derive(Debug, thiserror::Error)]
pub enum BigQuerySourceError {
    #[error("BigQuery: {0}")]
    Api(#[from] ApiError),
    #[error("{0}")]
    Invalid(String),
    /// What the caller's batch consumer failed with, as it said it.
    #[error("{0}")]
    Consumer(String),
}
