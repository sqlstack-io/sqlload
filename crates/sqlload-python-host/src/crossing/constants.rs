use crate::crossing::models::DictCrossing;

/// Dict rows gathered before they cross into Rust (and the most rows per batch parsed from bytes): as fast as 10,000 in the crossing benchmark, with a third of the memory.
pub const DEFAULT_BATCH_ROWS: usize = 1_000;

/// Batches queued between the generator, the parser and the consumer.
pub const DEFAULT_MAX_QUEUED: usize = 4;

/// How dicts cross by default: serialised by orjson and parsed in parallel without the GIL, which the crossing benchmark found faster than walking them and holding the GIL a third as long.
pub const DEFAULT_DICT_CROSSING: DictCrossing = DictCrossing::Serialise;

/// The most threads parsing one table's batches by default.
pub const MAX_DEFAULT_PARSE_THREADS: usize = 4;

/// Bytes of crossed data waiting to be parsed before the generator pauses.
pub const DEFAULT_MAX_QUEUED_BYTES: usize = 256 * 1024 * 1024;

/// The module of numpy's scalar and array types, which orjson writes by their own rules.
pub(crate) const NUMPY_MODULE: &str = "numpy";

/// numpy's array type, written as a list when its values are numbers or booleans.
pub(crate) const NUMPY_ARRAY: &str = "ndarray";

/// `date(1970, 1, 1).toordinal()`.
pub(crate) const EPOCH_ORDINAL: i64 = 719_163;

/// Microseconds in a day.
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Microseconds in a second.
pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;

/// The `sqlload_api._host` function orjson calls for values it cannot write itself.
pub(crate) const CROSSING_DEFAULT: &str = "crossing_default";

/// How long a page `transform`'s crossing is waited on at a time; it polls again until the function's output ends.
pub(crate) const PAGE_POLL: std::time::Duration = std::time::Duration::from_secs(3600);
