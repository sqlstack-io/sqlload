//! Discover the topics to export.

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;

use crate::config::models::Config;
use crate::kafka::_helpers::client::client_config;
use crate::kafka::constants::METADATA_TIMEOUT;
use crate::kafka::main::select_topics::select_topics;

/// Every topic matching `include` and not `exclude`, skipping internal `__` topics, sorted.
pub async fn list_topics(config: &Config) -> Result<Vec<String>, String> {
    let client = client_config(config);
    let names = tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
        let consumer: BaseConsumer = client
            .create()
            .map_err(|error| format!("cannot connect to Kafka: {error}"))?;
        let metadata = consumer
            .fetch_metadata(None, Timeout::After(METADATA_TIMEOUT))
            .map_err(|error| format!("cannot list Kafka topics: {error}"))?;
        Ok(metadata
            .topics()
            .iter()
            .map(|topic| topic.name().to_owned())
            .collect())
    })
    .await
    .map_err(|error| format!("topic listing task failed: {error}"))??;
    select_topics(config, names)
}
