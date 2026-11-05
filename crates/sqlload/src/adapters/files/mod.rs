//! Parquet files in an object store or directory as a destination: table paths, partitions, live schemas and writing.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
