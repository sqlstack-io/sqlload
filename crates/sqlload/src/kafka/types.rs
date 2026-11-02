use crate::kafka::models::RebalanceForwarder;

/// The consumer type every stream uses.
pub type StreamKafkaConsumer = rdkafka::consumer::StreamConsumer<RebalanceForwarder>;
