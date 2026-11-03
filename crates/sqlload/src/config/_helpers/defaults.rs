use std::time::Duration;

use crate::config::constants::{
    DEFAULT_CONSUMER_GROUP, DEFAULT_DISCOVERY_INTERVAL_SECS, DEFAULT_MAX_AGE_SECS,
    DEFAULT_MAX_BYTES, DEFAULT_MAX_IN_FLIGHT, DEFAULT_MAX_REQUEST_BYTES, DEFAULT_MAX_ROWS,
    DEFAULT_MAX_TOTAL_BYTES, DEFAULT_PIPE_TEMPLATE, DEFAULT_PREFETCH_BUDGET, DEFAULT_RETAIN_SECS,
    DEFAULT_S3_PATH, DEFAULT_STAGE_TEMPLATE, DEFAULT_STARTUP_BATCH, DEFAULT_STARTUP_INTERVAL_MS,
    DEFAULT_STREAMING_PIPE_TEMPLATE,
};
use crate::config::models::{ByteSize, ColumnConfig, EnvelopeField};

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

pub(crate) fn columns() -> Vec<ColumnConfig> {
    [
        ("TOPIC", EnvelopeField::Topic, "VARCHAR"),
        ("KAFKA_TIMESTAMP", EnvelopeField::Timestamp, "NUMBER(38,0)"),
        (
            "KAFKA_TIMESTAMP_TYPE",
            EnvelopeField::TimestampType,
            "NUMBER(38,0)",
        ),
        ("KAFKA_OFFSET", EnvelopeField::Offset, "NUMBER(38,0)"),
        ("KAFKA_PARTITION", EnvelopeField::Partition, "NUMBER(38,0)"),
        ("KEY", EnvelopeField::Key, "VARCHAR"),
        ("VALUE", EnvelopeField::Value, "VARCHAR"),
        ("EXPORTED_AT", EnvelopeField::ExportedAt, "TIMESTAMP_TZ"),
        ("DATE", EnvelopeField::Date, "DATE"),
    ]
    .into_iter()
    .map(|(name, field, sql_type)| ColumnConfig {
        name: name.to_owned(),
        field,
        sql_type: sql_type.to_owned(),
    })
    .collect()
}
