//! The time leases are taken, kept and judged at.

use chrono::{DateTime, Utc};

/// Now, for leases: the clock, moved on by `SQLLOAD_TEST_LEASE_OFFSET_SECONDS` in test builds, so a crash test's next load runs as if a crashed load's lease had run out.
pub fn lease_now() -> DateTime<Utc> {
    #[cfg(feature = "container-tests")]
    if let Ok(seconds) = std::env::var("SQLLOAD_TEST_LEASE_OFFSET_SECONDS")
        .unwrap_or_default()
        .parse::<i64>()
    {
        return Utc::now() + chrono::Duration::seconds(seconds);
    }
    Utc::now()
}
