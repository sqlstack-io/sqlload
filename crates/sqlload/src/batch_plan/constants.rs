/// A batch commits once its staged data reaches about this many bytes (1 GB of Parquet; Arrow bytes where nothing is encoded).
pub const BATCH_BYTES: usize = 1000 * 1000 * 1000;

/// The most source files in one batch of a files source.
pub const MAX_BATCH_FILES: usize = 1000;

/// Seconds in a day: a date cursor's `batch_size` is a whole number of days.
pub const SECONDS_PER_DAY: u64 = 86_400;

/// The most windows one plan may split a table's read into; a smaller `batch_size` over a longer range is refused.
pub const MAX_WINDOWS: u64 = 100_000;

/// The offset suffix of cursor bounds in UTC, as `cursor_text` writes them.
pub const UTC_OFFSET_SUFFIX: &str = "+00";

/// Microseconds in a day, for the whole-day rule on rows outside a window.
pub const DAY_MICROS: i64 = 86_400_000_000;

/// Microseconds in half an hour, the unit an offset outside a window is counted in.
pub const HALF_HOUR_MICROS: i64 = 1_800_000_000;

/// The widest offset between time zones, in half hours (14 hours), beyond which rows outside a window are not put down to one.
pub const MAX_OFFSET_HALF_HOURS: i64 = 28;
