use std::time::Duration;

pub const CONFLICT: u16 = 409;
/// How far back stage cleanup looks for dated folders beyond the retention.
pub const PRUNE_LOOKBACK: Duration = Duration::from_secs(30 * 86_400);
