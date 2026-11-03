//! A streaming-mode configuration against a test broker and the fake service.

use sqlload::config::main::parse_config::parse_config;
use sqlload::config::models::Config;

/// Streaming delivery from `brokers` topics matching `include` to the fake at `base_url`, with `extra` TOML appended.
pub fn streaming_config(
    brokers: &str,
    base_url: &str,
    include: &str,
    extra: &str,
) -> Result<Config, String> {
    let text = format!(
        r#"
[source]
kind = "kafka"
brokers = "{brokers}"
include = ["{include}"]
start_from = "earliest"
discovery_interval = "2s"

[destination]
kind = "snowflake"
account = "test-account"
user = "LOADER"
auth = {{ method = "pat", token = "test-token" }}
database = "TEST_DB"
schema = "RAW"
base_url = "{base_url}"

[staging]
kind = "none"
{extra}
"#
    );
    parse_config(&text, &|_| None).map_err(|error| error.to_string())
}
