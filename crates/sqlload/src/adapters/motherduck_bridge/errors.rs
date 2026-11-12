/// Why a MotherDuck bridge request failed.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// The bridge could not start, or its Python cannot run it.
    #[error("{0}")]
    Setup(String),
    /// The bridge process ended; any request still waiting fails with this.
    #[error("{0}")]
    Stopped(String),
    /// DuckDB refused the request, in its own words.
    #[error("{0}")]
    Engine(String),
}
