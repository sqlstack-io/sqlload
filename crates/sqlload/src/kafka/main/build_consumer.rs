//! Create a stream's consumer.

use rdkafka::consumer::Consumer;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::config::models::{Config, StartFrom};
use crate::kafka::_helpers::client::client_config;
use crate::kafka::constants::DEFAULT_FETCH_WAIT_MS;
use crate::kafka::models::{RebalanceEvent, RebalanceForwarder};
use crate::kafka::types::StreamKafkaConsumer;
use crate::naming::models::Stream;

/// A consumer in the stream's group with offsets committed only by sqlload; properties override the prefetch and fetch wait.
pub fn build_consumer(
    config: &Config,
    stream: &Stream,
    start_from: StartFrom,
    prefetch_kbytes: u64,
) -> Result<(StreamKafkaConsumer, UnboundedReceiver<RebalanceEvent>), String> {
    let (sender, receiver) = unbounded_channel();
    let reset = match start_from {
        StartFrom::Earliest => "earliest",
        StartFrom::Latest => "latest",
    };
    let mut client = client_config(config);
    let defaults = [
        ("queued.max.messages.kbytes", prefetch_kbytes.to_string()),
        ("fetch.wait.max.ms", DEFAULT_FETCH_WAIT_MS.to_owned()),
    ];
    for (key, value) in defaults {
        if !config.source.properties.contains_key(key) {
            client.set(key, value);
        }
    }
    let consumer: StreamKafkaConsumer = client
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
