//! SQLLoad: the Kafka to Snowflake exporter and the loader engine.

#![forbid(unsafe_code)]

pub mod adapters;
pub mod auth;
pub mod batch_plan;
pub mod batching;
pub mod bigquery_api;
pub mod cdc;
pub mod column_types;
/// The `sqlload` command line; behind the default `cli` feature.
#[cfg(feature = "cli")]
pub mod command;
pub mod config;
pub mod contract;
pub mod copy;
pub mod databricks_api;
pub mod delivery;
pub mod envelope;
pub mod exporter;
pub mod file_format;
pub mod file_store;
pub mod files_source;
pub mod http;
pub mod http_source;
pub mod kafka;
pub mod loader;
pub mod masking;
pub mod metrics;
pub mod naming;
pub mod parquet_parts;
pub mod planning;
pub mod postgres_source;
pub mod project;
pub mod python_sources;
pub mod routing;
pub mod row_batches;
pub mod run_log;
pub mod schema_plan;
pub mod similar_names;
pub mod snowflake_source;
pub mod sql_api;
pub mod stage_api;
pub mod status;
pub mod store_paths;
pub mod streaming_api;
pub mod table_lease;
pub mod time_text;
