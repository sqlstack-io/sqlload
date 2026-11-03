use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::auth::models::Secret;
use crate::config::_helpers::defaults;
use crate::config::_helpers::units::{
    deserialize_duration, deserialize_optional_duration, parse_byte_size,
};

/// The whole `sqlload.toml` file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source: SourceConfig,
    pub destination: DestinationConfig,
    pub staging: StagingConfig,
    #[serde(default)]
    pub batching: BatchingConfig,
    #[serde(default)]
    pub envelope: EnvelopeConfig,
    #[serde(default)]
    pub naming: NamingConfig,
    #[serde(default)]
    pub topics: Vec<TopicOverride>,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

/// Where records come from.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub kind: SourceKind,
    pub brokers: String,
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default = "defaults::consumer_group")]
    pub consumer_group: String,
    #[serde(default)]
    pub start_from: StartFrom,
    #[serde(
        default = "defaults::discovery_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub discovery_interval: Duration,
    /// How many instances split the streams between them.
    #[serde(default = "defaults::shards")]
    pub shards: u32,
    /// This instance's index below `shards`; `run --shard` sets it.
    #[serde(default)]
    pub shard: u32,
    #[serde(default = "defaults::prefetch_budget")]
    pub prefetch_budget: ByteSize,
    #[serde(default = "defaults::startup_batch")]
    pub startup_batch: usize,
    #[serde(
        default = "defaults::startup_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub startup_interval: Duration,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// The supported source systems.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Kafka,
}

/// Where a consumer group with no committed offset starts reading.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    Earliest,
    #[default]
    Latest,
}

/// The warehouse records are written to.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub kind: DestinationKind,
    pub account: String,
    pub user: String,
    pub auth: AuthConfig,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub warehouse: Option<String>,
    pub database: String,
    pub schema: String,
    #[serde(default)]
    pub base_url: Option<String>,
}

/// The supported warehouses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationKind {
    Snowflake,
}

/// How sqlload authenticates to the warehouse.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthConfig {
    Pat { token: Secret },
    KeyPair { private_key_file: PathBuf },
}

/// How batches reach the warehouse.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagingConfig {
    pub kind: StagingKind,
    #[serde(
        default = "defaults::retain",
        deserialize_with = "deserialize_duration"
    )]
    pub retain: Duration,
    #[serde(default)]
    pub load: LoadMethod,
    /// The staged files' format.
    #[serde(default)]
    pub format: FileFormat,
    /// The most uploads, and separately the most loads, in flight at once across all streams; also the most files per stream awaiting a Snowpipe load report.
    #[serde(default = "defaults::max_in_flight")]
    pub max_in_flight: usize,
    /// What happens when Snowpipe reports that an `insertFiles` file failed to load.
    #[serde(default)]
    pub on_load_error: LoadErrorPolicy,
    #[serde(default)]
    pub s3: Option<S3Config>,
}

/// What happens to a staged file Snowpipe failed to load, fully or partly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadErrorPolicy {
    /// Stop the stream without committing the file's offsets.
    #[default]
    Halt,
    /// Log it, count it and commit past it.
    Skip,
}

/// How staged files are encoded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// Gzip newline-delimited JSON, one object per row.
    #[default]
    Json,
    /// Zstd Parquet with the envelope columns; `exported_at` is a UTC timestamp.
    Parquet,
}

/// The delivery modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingKind {
    None,
    Internal,
    S3,
}

/// How staged files are loaded into tables.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadMethod {
    #[default]
    Auto,
    Snowpipe,
    Copy,
}

/// The bucket used by the `s3` delivery mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub region: String,
    /// The external stage, `NAME` in the destination schema or `DATABASE.SCHEMA.NAME`.
    pub stage: String,
    /// Folder template under `prefix`; see [`crate::config::constants::DEFAULT_S3_PATH`].
    #[serde(default = "defaults::s3_path")]
    pub path: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub path_style: bool,
    #[serde(default = "defaults::auto_ingest")]
    pub auto_ingest: bool,
}

/// When a stream's buffered records become one batch.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchingConfig {
    #[serde(default = "defaults::max_bytes")]
    pub max_bytes: ByteSize,
    #[serde(default = "defaults::max_rows")]
    pub max_rows: usize,
    #[serde(
        default = "defaults::max_age",
        deserialize_with = "deserialize_duration"
    )]
    pub max_age: Duration,
    #[serde(default = "defaults::max_total_bytes")]
    pub max_total_bytes: ByteSize,
    #[serde(default = "defaults::max_request_bytes")]
    pub max_request_bytes: ByteSize,
    #[serde(default)]
    pub oversized: OversizedPolicy,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_bytes: defaults::max_bytes(),
            max_rows: defaults::max_rows(),
            max_age: defaults::max_age(),
            max_total_bytes: defaults::max_total_bytes(),
            max_request_bytes: defaults::max_request_bytes(),
            oversized: OversizedPolicy::default(),
        }
    }
}

/// What happens to a record that cannot fit in one streaming request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OversizedPolicy {
    #[default]
    Fail,
    Skip,
}

/// The columns every exported row carries.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvelopeConfig {
    #[serde(default = "defaults::columns")]
    pub columns: Vec<ColumnConfig>,
    #[serde(default = "defaults::error_logging")]
    pub error_logging: bool,
    /// IANA zone whose UTC offset `exported_at` values carry, e.g. `America/Los_Angeles`.
    #[serde(default = "defaults::time_zone")]
    pub time_zone: String,
}

impl Default for EnvelopeConfig {
    fn default() -> Self {
        Self {
            columns: defaults::columns(),
            error_logging: defaults::error_logging(),
            time_zone: defaults::time_zone(),
        }
    }
}

/// One envelope column.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    pub name: String,
    pub field: EnvelopeField,
    #[serde(rename = "type")]
    pub sql_type: String,
}

/// The record attribute a column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeField {
    Topic,
    Partition,
    Offset,
    Key,
    Timestamp,
    TimestampType,
    Value,
    Headers,
    ExportedAt,
    Date,
    Null,
}

/// How topics map to tables and object names.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamingConfig {
    #[serde(default)]
    pub strip_prefix: Option<String>,
    /// Put in front of every table name; `{stream}` and consumer groups leave it out.
    #[serde(default)]
    pub table_prefix: String,
    /// The table for topics whose derived name is not a valid identifier, instead of failing.
    #[serde(default)]
    pub fallback: Option<String>,
    #[serde(default)]
    pub rules: Vec<NamingRule>,
    #[serde(default)]
    pub map: BTreeMap<String, String>,
    #[serde(default)]
    pub unmatched: UnmatchedPolicy,
    #[serde(default = "defaults::stage_template")]
    pub stage: String,
    #[serde(default = "defaults::pipe_template")]
    pub pipe: String,
    #[serde(default = "defaults::streaming_pipe_template")]
    pub streaming_pipe: String,
}

impl Default for NamingConfig {
    fn default() -> Self {
        Self {
            strip_prefix: None,
            table_prefix: String::new(),
            fallback: None,
            rules: Vec::new(),
            map: BTreeMap::new(),
            unmatched: UnmatchedPolicy::default(),
            stage: defaults::stage_template(),
            pipe: defaults::pipe_template(),
            streaming_pipe: defaults::streaming_pipe_template(),
        }
    }
}

/// A pattern rule: named captures in `match` fill the `table` template, or `skip` leaves the topic out.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamingRule {
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub skip: bool,
}

/// What happens to a topic that no rule or map entry names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnmatchedPolicy {
    #[default]
    Default,
    Fail,
}

/// Settings for topics matching a glob; later entries win.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicOverride {
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default)]
    pub start_from: Option<StartFrom>,
    #[serde(default)]
    pub max_bytes: Option<ByteSize>,
    #[serde(default)]
    pub max_rows: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub max_age: Option<Duration>,
    #[serde(default)]
    pub oversized: Option<OversizedPolicy>,
    /// Columns filled from a member of the JSON value instead, e.g. `{ VALUE = "value" }`.
    #[serde(default)]
    pub unwrap: BTreeMap<String, String>,
}

/// The optional metrics and health endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub listen: Option<SocketAddr>,
}

/// The effective settings of one stream after topic overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSettings {
    pub start_from: StartFrom,
    pub max_bytes: u64,
    pub max_rows: usize,
    pub max_age: Duration,
    pub oversized: OversizedPolicy,
    pub unwrap: BTreeMap<String, String>,
}

/// A byte count written as an integer or as text such as `32MB` or `3.5MiB`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawSize {
    Number(u64),
    Text(String),
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match RawSize::deserialize(deserializer)? {
            RawSize::Number(bytes) => Ok(Self(bytes)),
            RawSize::Text(text) => parse_byte_size(&text)
                .map(Self)
                .map_err(serde::de::Error::custom),
        }
    }
}
