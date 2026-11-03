use rdkafka::ClientConfig;

use crate::config::models::Config;

/// Shared client settings: brokers plus every `[source.properties]` entry.
pub(crate) fn client_config(config: &Config) -> ClientConfig {
    let mut client = ClientConfig::new();
    client.set("bootstrap.servers", &config.source.brokers);
    for (key, value) in &config.source.properties {
        client.set(key, value);
    }
    client
}
