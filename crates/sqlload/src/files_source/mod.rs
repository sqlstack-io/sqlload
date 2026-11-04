//! Existing files as a source: listing, partition values, schemas from Parquet footers, and reading into Arrow.

pub(crate) mod _helpers;
pub mod constants;
pub mod errors;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
