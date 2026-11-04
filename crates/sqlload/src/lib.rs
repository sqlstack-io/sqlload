//! SQLLoad: the Kafka to Snowflake exporter and the loader engine.

#![forbid(unsafe_code)]

pub mod auth;
pub mod batching;
pub mod column_types;
/// The `sqlload` command line; behind the default `cli` feature.
#[cfg(feature = "cli")]
pub mod command;
pub mod config;
pub mod delivery;
#[cfg(feature = "duckdb")]
pub mod duckdb_destination;
pub mod envelope;
pub mod exporter;
pub mod file_format;
pub mod file_store;
pub mod files_source;
pub mod http;
pub mod kafka;
pub mod loader;
pub mod masking;
pub mod metrics;
pub mod naming;
pub mod planning;
pub mod postgres_source;
pub mod project;
pub mod routing;
pub mod schema_plan;
pub mod similar_names;
pub mod snowflake_destination;
pub mod sql_api;
pub mod stage_api;
pub mod status;
pub mod store_paths;
pub mod streaming_api;
pub mod time_text;
pub mod write_sql;
