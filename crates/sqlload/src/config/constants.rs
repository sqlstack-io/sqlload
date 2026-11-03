/// Consumer group name template; `{stream}` is the lower-case table name.
pub const DEFAULT_CONSUMER_GROUP: &str = "sqlload-{stream}";
pub const DEFAULT_DISCOVERY_INTERVAL_SECS: u64 = 300;
/// Kafka prefetch shared by all consumers.
pub const DEFAULT_PREFETCH_BUDGET: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_STARTUP_BATCH: usize = 20;
pub const DEFAULT_STARTUP_INTERVAL_MS: u64 = 500;
/// Uploads, and separately loads, in flight at once across all streams.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 32;
/// Snowpipe files per stream sent but not yet committed.
pub const DEFAULT_MAX_PENDING_FILES: usize = 32;
pub const DEFAULT_PENDING_CHECK_AFTER_SECS: u64 = 5 * 60;
pub const DEFAULT_PENDING_RECHECK_EVERY_SECS: u64 = 10 * 60;
pub const DEFAULT_RETAIN_SECS: u64 = 7 * 24 * 3600;
pub const DEFAULT_MAX_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_MAX_ROWS: usize = 100_000;
pub const DEFAULT_MAX_AGE_SECS: u64 = 60;
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Streaming requests are capped at 4 MiB after compression; keep headroom.
pub const DEFAULT_MAX_REQUEST_BYTES: u64 = 3_500_000;
pub const DEFAULT_STAGE_TEMPLATE: &str = "{table}_SQLLOAD_STAGE";
pub const DEFAULT_PIPE_TEMPLATE: &str = "{table}_SQLLOAD_PIPE";
/// Snowflake creates this pipe for every table on first streaming use.
pub const DEFAULT_STREAMING_PIPE_TEMPLATE: &str = "{table}-STREAMING";
pub const STREAMING_REQUEST_LIMIT: u64 = 4 * 1024 * 1024;
pub const MAX_NAME_LENGTH: usize = 255;
/// Size units and their byte multipliers.
pub const SIZE_UNITS: &[(&str, f64)] = &[
    ("", 1.0),
    ("b", 1.0),
    ("k", 1e3),
    ("kb", 1e3),
    ("m", 1e6),
    ("mb", 1e6),
    ("g", 1e9),
    ("gb", 1e9),
    ("kib", 1024.0),
    ("mib", 1_048_576.0),
    ("gib", 1_073_741_824.0),
];
/// `{topic}`, `{table}`, `{stream}`, `{date}` (UTC `YYYY-MM-DD`) and `{hour}` (UTC `HH`).
pub const DEFAULT_S3_PATH: &str = "{topic}/date={date}/";
pub const S3_PATH_PLACEHOLDERS: &[&str] = &["topic", "table", "stream", "date", "hour"];
