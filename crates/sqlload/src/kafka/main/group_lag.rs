//! Consumer group lag for `status`.

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};

use crate::config::models::Config;
use crate::kafka::_helpers::client::client_config;
use crate::kafka::models::PartitionLag;

const TIMEOUT: Duration = Duration::from_secs(30);

/// Committed offsets and high watermarks of `group` on every partition of `topics`, read without joining the group.
pub async fn group_lag(
    config: &Config,
    group: &str,
    topics: &[String],
) -> Result<Vec<PartitionLag>, String> {
    let mut client = client_config(config);
    client.set("group.id", group);
    let topics = topics.to_vec();
    let group = group.to_owned();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = client
            .create()
            .map_err(|error| format!("cannot connect to Kafka: {error}"))?;
        let mut list = TopicPartitionList::new();
        for topic in &topics {
            let metadata = consumer
                .fetch_metadata(Some(topic), Timeout::After(TIMEOUT))
                .map_err(|error| format!("cannot read metadata for {topic}: {error}"))?;
            for partition in metadata
                .topics()
                .iter()
                .flat_map(|found| found.partitions())
            {
                list.add_partition(topic, partition.id());
            }
        }
        let committed = consumer
            .committed_offsets(list, Timeout::After(TIMEOUT))
            .map_err(|error| format!("cannot read committed offsets of {group}: {error}"))?;
        let mut lags: Vec<PartitionLag> = Vec::new();
        for element in committed.elements() {
            let (_, high) = consumer
                .fetch_watermarks(
                    element.topic(),
                    element.partition(),
                    Timeout::After(TIMEOUT),
                )
                .map_err(|error| format!("cannot read watermarks: {error}"))?;
            lags.push(PartitionLag {
                topic: element.topic().to_owned(),
                partition: element.partition(),
                committed: match element.offset() {
                    Offset::Offset(offset) => Some(offset),
                    _ => None,
                },
                high_watermark: high,
            });
        }
        Ok(lags)
    })
    .await
    .map_err(|error| format!("lag task failed: {error}"))?
}
