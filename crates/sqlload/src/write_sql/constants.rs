/// Suffix of the staging table a load writes into before its strategy runs.
pub const STAGING_SUFFIX: &str = "__sqlload_staging";

/// Suffix (after the staging table's name) of the table a `replace` builds and swaps in.
pub const SWAP_SUFFIX: &str = "_swap";

/// Date cursors move back by whole days: a lookback is rounded up to days.
pub const SECONDS_PER_DAY: u64 = 86_400;

/// How cursor timestamps are written in literals (microseconds, no offset).
pub const CURSOR_TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

/// Timestamps without an offset accepted on the command line.
pub const NAIVE_TIMESTAMP_FORMATS: &[&str] = &["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"];

/// Hours after which a staging table is a crashed load's, dropped at the start of the next load.
pub const STALE_STAGING_HOURS: i64 = 24;

/// How a staging table's name holds its load's start time.
pub const STAGING_TIME_FORMAT: &str = "%Y%m%d%H%M%S";

/// The length of that time.
pub const STAGING_TIME_DIGITS: usize = 14;
