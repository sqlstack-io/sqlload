//! Snowflake as a source: catalog discovery, and tables or queries unloaded as Parquet and read back as Arrow batches.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
