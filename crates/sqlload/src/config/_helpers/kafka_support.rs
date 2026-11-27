use crate::config::errors::ConfigError;

/// Whether an exporter configuration can run where Kafka support is `available` (built with the `kafka` feature): every one reads Kafka topics.
pub(crate) fn kafka_support(available: bool) -> Result<(), ConfigError> {
    match available {
        true => Ok(()),
        false => Err(ConfigError::KafkaUnavailable),
    }
}
