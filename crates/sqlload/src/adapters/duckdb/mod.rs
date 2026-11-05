//! DuckDB as a destination: in-process, used for local loads and fast real-engine tests. Its SQL dialect is always built; the engine itself only with the `duckdb` feature.

pub(crate) mod _helpers;
pub mod constants;
#[cfg(feature = "duckdb")]
pub mod errors;
#[cfg(feature = "duckdb")]
pub mod main;
pub mod models;
