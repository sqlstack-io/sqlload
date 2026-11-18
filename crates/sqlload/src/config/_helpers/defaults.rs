use std::time::Duration;

use crate::config::constants::{BIGQUERY_COLUMNS, DATABRICKS_COLUMNS, SNOWFLAKE_COLUMNS};
use crate::config::constants::{
    DEFAULT_CONSUMER_GROUP, DEFAULT_DISCOVERY_INTERVAL_SECS, DEFAULT_MAX_AGE_SECS,
    DEFAULT_MAX_BYTES, DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_PENDING_FILES, DEFAULT_MAX_REQUEST_BYTES,
    DEFAULT_MAX_ROWS, DEFAULT_MAX_TOTAL_BYTES, DEFAULT_PENDING_CHECK_AFTER_SECS,
    DEFAULT_PENDING_RECHECK_EVERY_SECS, DEFAULT_PIPE_TEMPLATE, DEFAULT_PREFETCH_BUDGET,
    DEFAULT_RETAIN_SECS, DEFAULT_S3_PATH, DEFAULT_STAGE_TEMPLATE, DEFAULT_STARTUP_BATCH,
    DEFAULT_STARTUP_INTERVAL_MS, DEFAULT_STREAMING_PIPE_TEMPLATE,
};
use crate::config::models::ByteSize;

pub(crate) fn consumer_group() -> String {
    DEFAULT_CONSUMER_GROUP.to_owned()
}

pub(crate) fn discovery_interval() -> Duration {
    Duration::from_secs(DEFAULT_DISCOVERY_INTERVAL_SECS)
}

pub(crate) fn shards() -> u32 {
    1
}

pub(crate) fn prefetch_budget() -> ByteSize {
    ByteSize(DEFAULT_PREFETCH_BUDGET)
}

pub(crate) fn startup_batch() -> usize {
    DEFAULT_STARTUP_BATCH
}

pub(crate) fn startup_interval() -> Duration {
    Duration::from_millis(DEFAULT_STARTUP_INTERVAL_MS)
}

pub(crate) fn retain() -> Duration {
    Duration::from_secs(DEFAULT_RETAIN_SECS)
}

pub(crate) fn max_in_flight() -> usize {
    DEFAULT_MAX_IN_FLIGHT
}

pub(crate) fn max_pending_files() -> usize {
    DEFAULT_MAX_PENDING_FILES
}

pub(crate) fn pending_check_after() -> Duration {
    Duration::from_secs(DEFAULT_PENDING_CHECK_AFTER_SECS)
}

pub(crate) fn pending_recheck_every() -> Duration {
    Duration::from_secs(DEFAULT_PENDING_RECHECK_EVERY_SECS)
}

pub(crate) fn auto_ingest() -> bool {
    false
}

pub(crate) fn s3_path() -> String {
    DEFAULT_S3_PATH.to_owned()
}

pub(crate) fn max_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_BYTES)
}

pub(crate) fn max_rows() -> usize {
    DEFAULT_MAX_ROWS
}

pub(crate) fn max_age() -> Duration {
    Duration::from_secs(DEFAULT_MAX_AGE_SECS)
}

pub(crate) fn max_total_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_TOTAL_BYTES)
}

pub(crate) fn max_request_bytes() -> ByteSize {
    ByteSize(DEFAULT_MAX_REQUEST_BYTES)
}

pub(crate) fn time_zone() -> String {
    "UTC".to_owned()
}

pub(crate) fn error_logging() -> bool {
    true
}

pub(crate) fn stage_template() -> String {
    DEFAULT_STAGE_TEMPLATE.to_owned()
}

pub(crate) fn pipe_template() -> String {
    DEFAULT_PIPE_TEMPLATE.to_owned()
}

pub(crate) fn streaming_pipe_template() -> String {
    DEFAULT_STREAMING_PIPE_TEMPLATE.to_owned()
}

/// Fill what defaults by destination before the TOML becomes a [`crate::config::models::Config`]: the envelope columns, and Parquet files on Databricks and BigQuery.
pub(crate) fn destination_defaults(value: &mut toml::Value) {
    let kind = value
        .get("destination")
        .and_then(|destination| destination.get("kind"))
        .and_then(toml::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let Some(root) = value.as_table_mut() else {
        return;
    };
    let parquet = matches!(kind.as_str(), "databricks" | "bigquery");
    if parquet && let Some(staging) = root.get_mut("staging").and_then(toml::Value::as_table_mut) {
        staging
            .entry("format")
            .or_insert_with(|| toml::Value::from("parquet"));
    }
    let envelope = root
        .entry("envelope")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    if let Some(envelope) = envelope.as_table_mut() {
        envelope.entry("columns").or_insert_with(|| {
            let columns = match kind.as_str() {
                "databricks" => DATABRICKS_COLUMNS,
                "bigquery" => BIGQUERY_COLUMNS,
                _ => SNOWFLAKE_COLUMNS,
            };
            toml::Value::Array(columns.iter().map(column_value).collect())
        });
    }
}

fn column_value((name, field, sql_type): &(&str, &str, &str)) -> toml::Value {
    toml::Value::Table(toml::Table::from_iter([
        ("name".to_owned(), toml::Value::from(*name)),
        ("field".to_owned(), toml::Value::from(*field)),
        ("type".to_owned(), toml::Value::from(*sql_type)),
    ]))
}
