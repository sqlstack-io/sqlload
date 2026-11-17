//! `sqlload playground`: a self-contained project of CSV files that loads into DuckDB, and the bundled changes that show an incremental load.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
