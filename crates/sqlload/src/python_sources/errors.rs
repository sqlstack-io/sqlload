/// Why a Python table's read failed, and whether running it again unchanged may succeed (a connection or timeout error, or an `sl.http` request still throttled or failing on the server after its retries).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ReadError {
    pub message: String,
    pub retryable: bool,
}

impl ReadError {
    /// A failure worth retrying unchanged.
    pub fn transient(message: String) -> Self {
        Self {
            message,
            retryable: true,
        }
    }
}

impl From<String> for ReadError {
    fn from(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}
