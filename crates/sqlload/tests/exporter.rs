//! Docker-backed exporter tests: a real Redpanda broker and the fake Snowflake service.

#[path = "exporter/helpers.rs"]
mod helpers;
#[path = "exporter/test_s3_delivery.rs"]
mod test_s3_delivery;
#[path = "exporter/test_sharding.rs"]
mod test_sharding;
#[path = "exporter/test_streaming_delivery.rs"]
mod test_streaming_delivery;
#[path = "exporter/test_streaming_recovery.rs"]
mod test_streaming_recovery;
#[path = "exporter/test_types.rs"]
mod test_types;
