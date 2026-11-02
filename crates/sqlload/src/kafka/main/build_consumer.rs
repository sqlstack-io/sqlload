//! Create a stream's consumer.

use rdkafka::consumer::Consumer;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::config::models::{Config, StartFrom};
use crate::kafka::_helpers::client::client_config;
use crate::kafka::models::{RebalanceEvent, RebalanceForwarder};
use crate::kafka::types::StreamKafkaConsumer;
use crate::naming::models::Stream;

/// A consumer in the stream's group, subscribed to its topics, with offsets committed only by sqlload.
pub fn build_consumer(
    config: &Config,
    stream: &Stream,
    start_from: StartFrom,
) -> Result<(StreamKafkaConsumer, UnboundedReceiver<RebalanceEvent>), String> {
    let (sender, receiver) = unbounded_channel();
    let reset = match start_from {
        StartFrom::Earliest => "earliest",
        StartFrom::Latest => "latest",
    };
    let consumer: StreamKafkaConsumer = client_config(config)
        .set("group.id", &stream.consumer_group)
        .set("client.id", format!("sqlload-{}", stream.name))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", reset)
        .set("max.poll.interval.ms", "900000")
        .create_with_context(RebalanceForwarder { events: sender })
        .map_err(|error| format!("cannot create the consumer for {}: {error}", stream.name))?;
    let topics: Vec<&str> = stream.topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topics)
        .map_err(|error| format!("cannot subscribe {} to {topics:?}: {error}", stream.name))?;
    Ok((consumer, receiver))
}
