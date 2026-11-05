//! Snowflake as a loader destination: Parquet files staged on the user stage, then `COPY` and SQL.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
