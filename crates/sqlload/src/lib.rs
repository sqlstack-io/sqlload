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
pub mod envelope;
pub mod exporter;
pub mod file_format;
pub mod http;
pub mod kafka;
pub mod metrics;
pub mod naming;
pub mod planning;
pub mod routing;
pub mod sql_api;
pub mod stage_api;
pub mod status;
pub mod streaming_api;
pub mod time_text;
