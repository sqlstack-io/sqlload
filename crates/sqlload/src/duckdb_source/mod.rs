//! DuckDB as a source: tables and views of a database file that `include` finds, and query tables, read as Arrow from a read-only connection.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
