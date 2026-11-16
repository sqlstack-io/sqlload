use crate::http::errors::ApiError;

/// Why reading a Databricks source failed.
#[derive(Debug, thiserror::Error)]
pub enum DatabricksSourceError {
    #[error("Databricks: {0}")]
    Api(#[from] ApiError),
    #[error("{0}")]
    Invalid(String),
    /// What the caller's batch consumer failed with, as it said it.
    #[error("{0}")]
    Consumer(String),
}
