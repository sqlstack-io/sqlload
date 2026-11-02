use crate::http::errors::ApiError;

/// Why a plan was not applied.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("existing objects do not fit the configuration:\n  - {}", .0.join("\n  - "))]
    Incompatible(Vec<String>),
    #[error("creating {name} failed: {source}")]
    Create { name: String, source: ApiError },
}
