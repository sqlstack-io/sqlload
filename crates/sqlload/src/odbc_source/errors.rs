/// Why reading through ODBC failed.
#[derive(Debug, thiserror::Error)]
pub enum OdbcSourceError {
    #[error("ODBC: {0}")]
    Odbc(#[from] arrow_odbc::odbc_api::Error),
    #[error("ODBC: {0}")]
    Arrow(#[from] arrow_odbc::Error),
    #[error("{0}")]
    Invalid(String),
    /// What the caller's batch consumer failed with, as it said it.
    #[error("{0}")]
    Consumer(String),
}
