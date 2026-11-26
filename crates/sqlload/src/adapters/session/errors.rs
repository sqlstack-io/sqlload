/// What kind of failure a caller's session reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionErrorKind {
    /// A failure a rerun will meet again: bad SQL, data, permissions.
    Failed,
    /// A failure a rerun may pass: a dropped connection or throttling, as the caller judges it.
    Transient,
    /// What a statement or query names does not exist, such as a BigQuery dataset before its first load: SQLLoad reads that as no table yet where it asks.
    NotFound,
}

/// Why the caller's session could not run a statement or take a file: the caller's own message, such as its driver's error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SessionError {
    pub message: String,
    pub kind: SessionErrorKind,
}

impl SessionError {
    /// A failure a rerun will meet again.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: SessionErrorKind::Failed,
        }
    }

    /// A failure a rerun may pass.
    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: SessionErrorKind::Transient,
        }
    }

    /// A failure because what the statement names does not exist.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: SessionErrorKind::NotFound,
        }
    }
}
