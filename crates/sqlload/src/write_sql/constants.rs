/// Suffix of the staging table a load writes into before its strategy runs.
pub const STAGING_SUFFIX: &str = "__sqlload_staging";

/// Date cursors move back by whole days: a lookback is rounded up to days.
pub const SECONDS_PER_DAY: u64 = 86_400;
