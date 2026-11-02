//! Produce test records.

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};

/// Create `topic` with `partitions` and produce `values` round-robin by key, waiting for each acknowledgement.
pub async fn produce_records(
    brokers: &str,
    topic: &str,
    partitions: i32,
    values: &[String],
) -> Result<(), String> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|error| error.to_string())?;
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let _ = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "30000")
        .create()
        .map_err(|error| error.to_string())?;
    for (index, value) in values.iter().enumerate() {
        let key = format!("key-{}", index % 7);
        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(value)
            .partition((index as i32) % partitions);
        producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(error, _)| error.to_string())?;
    }
    Ok(())
}
