use std::time::Duration;

pub const CONFLICT: u16 = 409;
/// How far back stage cleanup looks for dated folders beyond the retention.
pub const PRUNE_LOOKBACK: Duration = Duration::from_secs(30 * 86_400);
/// A file Snowpipe has not reported this long after it was sent is checked and sent again, then failed.
pub const UNREPORTED_AFTER: Duration = Duration::from_secs(5 * 60);
/// How far before a file was sent a history scan looks for its load.
pub const HISTORY_MARGIN: Duration = Duration::from_secs(60);
/// How far back a history scan looks for a file the pipe had received before this run sent it.
pub const RECEIVED_BEFORE_LOOKBACK: Duration = Duration::from_secs(24 * 3600);
/// The most history scan pages read for one check.
pub const MAX_HISTORY_PAGES: usize = 10;
