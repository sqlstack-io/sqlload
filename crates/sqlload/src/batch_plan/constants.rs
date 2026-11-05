/// A batch commits once its staged data reaches about this many bytes (1 GB of Parquet; Arrow bytes where nothing is encoded).
pub const BATCH_BYTES: usize = 1000 * 1000 * 1000;

/// The most source files in one batch of a files source.
pub const MAX_BATCH_FILES: usize = 1000;

/// Seconds in a day: a date cursor's `batch` is a whole number of days.
pub const SECONDS_PER_DAY: u64 = 86_400;
