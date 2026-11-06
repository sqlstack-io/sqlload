//! Databricks as a loader destination: Parquet files uploaded to a Unity Catalog volume, then `COPY INTO` and SQL over the Statement Execution API.

pub(crate) mod _helpers;
pub mod constants;
pub mod main;
pub mod models;

#[cfg(test)]
mod tests;
