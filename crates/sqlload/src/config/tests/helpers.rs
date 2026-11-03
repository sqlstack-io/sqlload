pub(crate) const BASE: &str = r#"
[source]
kind = "kafka"
brokers = "${BROKERS}"
include = ["orders.*"]

[destination]
kind = "snowflake"
account = "myorg-myaccount"
user = "LOADER"
auth = { method = "pat", token = "${TOKEN}" }
database = "MY_DB"
schema = "RAW"
"#;

/// Test environment variables.
pub(crate) fn lookup(name: &str) -> Option<String> {
    [("BROKERS", "localhost:9092"), ("TOKEN", "secret")]
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, value)| (*value).to_owned())
}
