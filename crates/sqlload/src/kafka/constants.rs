use std::time::Duration;

pub const METADATA_TIMEOUT: Duration = Duration::from_secs(30);
/// librdkafka's own per-consumer prefetch default; a budget share never exceeds it.
pub const MAX_PREFETCH_KBYTES: u64 = 65_536;
/// Below this a consumer cannot hold one default-sized fetch.
pub const MIN_PREFETCH_KBYTES: u64 = 1_024;
/// How long a fetch waits on partitions with no new data.
pub const DEFAULT_FETCH_WAIT_MS: &str = "2000";
