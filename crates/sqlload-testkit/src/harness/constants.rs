use std::time::Duration;

/// How long a test waits for rows before failing.
pub const WAIT: Duration = Duration::from_secs(90);
pub const POLL: Duration = Duration::from_millis(50);
