//! Read a consumer group's committed offsets.

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::redpanda::constants::{OFFSET_ATTEMPTS, OFFSET_RETRY_DELAY};

/// The sum of `group`'s committed offsets on every partition of `topic`, which is how many records it has committed when offsets start at 0; retried while the group coordinator moves.
pub async fn committed_offsets(brokers: &str, group: &str, topic: &str) -> Result<i64, String> {
    let brokers = brokers.to_owned();
    let group = group.to_owned();
    let topic = topic.to_owned();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .create()
            .map_err(|error| error.to_string())?;
        let mut last_error = String::new();
        for _ in 0..OFFSET_ATTEMPTS {
            match read_committed(&consumer, &topic) {
                Ok(total) => return Ok(total),
                Err(error) => last_error = error,
            }
            std::thread::sleep(OFFSET_RETRY_DELAY);
        }
        Err(last_error)
    })
    .await
    .map_err(|error| error.to_string())?
}

fn read_committed(consumer: &BaseConsumer, topic: &str) -> Result<i64, String> {
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|error| error.to_string())?;
    let mut list = TopicPartitionList::new();
    for found in metadata.topics() {
        for partition in found.partitions() {
            list.add_partition(topic, partition.id());
        }
    }
    let committed = consumer
        .committed_offsets(list, Duration::from_secs(10))
        .map_err(|error| error.to_string())?;
    Ok(committed
        .elements()
        .iter()
        .map(|element| match element.offset() {
            Offset::Offset(offset) => offset,
            _ => 0,
        })
        .sum())
}
