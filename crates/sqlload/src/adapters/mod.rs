//! Warehouse adapters, as sqlbuild's: the contract every warehouse implements, one folder per warehouse, and the one place that picks between them.

pub mod bigquery;
pub mod contract;
pub mod databricks;
pub mod dispatch;
pub mod duckdb;
pub mod files;
pub mod snowflake;
