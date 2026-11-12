//! Warehouse adapters, as sqlbuild's: the contract every warehouse implements, one folder per warehouse, and the one place that picks between them.

pub mod bigquery;
pub mod clickhouse;
pub mod contract;
pub mod databricks;
pub mod dispatch;
pub mod duckdb;
pub mod files;
#[cfg(feature = "duckdb")]
pub mod motherduck_bridge;
pub mod postgres;
pub mod snowflake;
