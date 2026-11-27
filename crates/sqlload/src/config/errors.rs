/// Why a configuration file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid TOML: {0}")]
    Syntax(String),
    #[error("{0}")]
    Interpolation(String),
    #[error("invalid configuration:\n  - {}", .0.join("\n  - "))]
    Invalid(Vec<String>),
    #[error(
        "this is an exporter configuration, which reads Kafka topics, and this build of SQLLoad has no Kafka support (it was built without the `kafka` feature); run it with the `sqlload` command or Python package, which include it"
    )]
    KafkaUnavailable,
}
