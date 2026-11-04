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
