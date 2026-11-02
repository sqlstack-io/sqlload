use std::time::Duration;

/// Scoped ingest tokens last an hour; refresh well before.
pub const SCOPED_TOKEN_LIFETIME: Duration = Duration::from_secs(45 * 60);
pub const UNAUTHORIZED: u16 = 401;
