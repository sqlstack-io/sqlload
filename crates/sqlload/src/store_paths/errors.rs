/// Why a path cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    #[error("path {path:?} contains a \"..\" segment; paths cannot leave their base")]
    ParentSegment { path: String },
    #[error("path {path:?} is empty, which would point at the whole {store}")]
    Empty { path: String, store: &'static str },
    #[error("path {path:?} contains a control character")]
    ControlCharacter { path: String },
    #[error(
        "{url:?} is not a location SQLLoad understands; use s3://bucket/path, file:///abs/path or an absolute path"
    )]
    UnknownScheme { url: String },
    #[error(
        "{url:?} points at {found}, but connection {connection:?} is {expected}; give a path relative to the connection"
    )]
    ConnectionMismatch {
        url: String,
        connection: String,
        found: String,
        expected: String,
    },
}
