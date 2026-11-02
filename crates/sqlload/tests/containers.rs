//! Docker-backed tests: a real Redpanda broker and the fake Snowflake service.

#[path = "containers/helpers.rs"]
mod helpers;
#[path = "containers/test_streaming_delivery.rs"]
mod test_streaming_delivery;
#[path = "containers/test_streaming_recovery.rs"]
mod test_streaming_recovery;
#[path = "containers/test_types.rs"]
mod test_types;
