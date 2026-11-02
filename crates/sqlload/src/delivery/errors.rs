/// Why a delivery did not complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryError {
    /// Channel state is unknown; reopen channels and resume from Snowflake's committed offsets.
    #[error("delivery must restart from committed offsets: {0}")]
    Reset(String),
    /// Retry the same batch after a backoff.
    #[error("delivery failed, will retry: {0}")]
    Retry(String),
    /// Stop the exporter; retrying cannot help.
    #[error("{0}")]
    Fatal(String),
}
