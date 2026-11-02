//! Discover the topics to export.

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;

use crate::config::models::Config;
use crate::kafka::_helpers::client::client_config;
use crate::kafka::constants::METADATA_TIMEOUT;

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
    let include = glob_set(&config.source.include)?;
    let exclude = glob_set(&config.source.exclude)?;
    let mut selected: Vec<String> = names
        .into_iter()
        .filter(|name| !name.starts_with("__") && include.is_match(name) && !exclude.is_match(name))
        .collect();
    selected.sort();
    Ok(selected)
}

fn glob_set(patterns: &[String]) -> Result<globset::GlobSet, String> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            globset::Glob::new(pattern)
                .map_err(|error| format!("invalid pattern {pattern:?}: {error}"))?,
        );
    }
    builder.build().map_err(|error| error.to_string())
}
