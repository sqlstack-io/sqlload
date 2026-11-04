/// Why a project file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum ProjectError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid TOML: {0}")]
    Syntax(String),
    #[error("{0}")]
    Interpolation(String),
    #[error("invalid project:\n  - {}", .0.join("\n  - "))]
    Invalid(Vec<String>),
}
