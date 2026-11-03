//! The current time.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, or 0 if the clock is before it.
pub fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_millis() as i64,
        Err(_) => 0,
    }
}
