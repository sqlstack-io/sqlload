//! Arrow batches written as Parquet files of a bounded size, for staging: row groups of at most 128 MiB or 1M rows, files of about 100 MB.

pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
