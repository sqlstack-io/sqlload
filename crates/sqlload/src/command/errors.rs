/// Why a loader command could not finish, and whether running it again unchanged may succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandError {
    pub message: String,
    pub retryable: bool,
}

impl From<String> for CommandError {
    fn from(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}

impl From<crate::loader::errors::LoaderError> for CommandError {
    fn from(error: crate::loader::errors::LoaderError) -> Self {
        Self {
            retryable: error.is_retryable(),
            message: error.to_string(),
        }
    }
}
