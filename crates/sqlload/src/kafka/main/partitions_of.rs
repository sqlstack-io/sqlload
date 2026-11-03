//! Read partition lists.

use crate::batching::models::Partition;

/// The partitions in a Kafka partition list.
pub fn partitions_of(list: &rdkafka::TopicPartitionList) -> Vec<Partition> {
    list.elements()
        .iter()
        .map(|element| Partition {
            topic: element.topic().to_owned(),
            partition: element.partition(),
        })
        .collect()
}
