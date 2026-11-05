//! Warehouse adapters, as sqlbuild's: the contract every warehouse implements, one folder per warehouse, and the one place that picks between them.

pub mod contract;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod snowflake;
