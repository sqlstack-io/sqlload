/// A failed call to a warehouse or object-store endpoint.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiError {
    #[error("request failed: {0}")]
    Transport(String),
    #[error("HTTP {status} {code}: {message}")]
    Status {
        status: u16,
        code: String,
        message: String,
    },
    #[error("unexpected response: {0}")]
    Protocol(String),
    #[error("{0}")]
    Auth(String),
}

impl ApiError {
    /// True for failures worth retrying unchanged: transport errors, throttling and server errors.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status { status, .. } => *status == 429 || *status >= 500,
            Self::Protocol(_) | Self::Auth(_) => false,
        }
    }

    /// The HTTP status, when the server answered.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The service error code, when the server sent one.
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Status { code, .. } => Some(code.as_str()),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error.without_url().to_string())
    }
}
