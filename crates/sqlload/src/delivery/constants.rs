use std::time::Duration;

pub const CONFLICT: u16 = 409;
/// How far back stage cleanup looks for dated folders beyond the retention.
pub const PRUNE_LOOKBACK: Duration = Duration::from_secs(30 * 86_400);
/// The first delay before retrying a load check that could not run; it doubles up to `pending_recheck_every`.
pub const CHECK_RETRY: Duration = Duration::from_secs(60);
/// How often a stage delivery reads Snowpipe's load report.
pub const STAGE_CONFIRM_POLL: Duration = Duration::from_secs(10);
/// How often a streaming delivery checks channel status.
pub const STREAMING_CONFIRM_POLL: Duration = Duration::from_secs(1);
/// The HTTP status Snowpipe answers when the role lacks a privilege on the pipe.
pub const FORBIDDEN: u16 = 403;
/// How far before a file was sent a history scan looks for its load.
pub const HISTORY_MARGIN: Duration = Duration::from_secs(60);
/// How far back a history scan looks for a file the pipe had received before this run sent it.
pub const RECEIVED_BEFORE_LOOKBACK: Duration = Duration::from_secs(24 * 3600);
/// The most history scan pages read for one check.
pub const MAX_HISTORY_PAGES: usize = 10;
