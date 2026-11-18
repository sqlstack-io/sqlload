//! DuckDB as a source: tables and views of a database file that `include` finds, and query tables, read as Arrow from a read-only connection. Only the constants are compiled without the `duckdb` feature, so plans can name the system.

#[cfg(feature = "duckdb")]
pub(crate) mod _helpers;
pub mod constants;
#[cfg(feature = "duckdb")]
pub mod errors;
#[cfg(feature = "duckdb")]
pub mod main;
#[cfg(feature = "duckdb")]
pub mod models;

#[cfg(all(test, feature = "duckdb"))]
mod tests;
