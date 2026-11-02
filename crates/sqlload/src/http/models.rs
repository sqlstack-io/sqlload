use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// How many new connections the shared HTTP client has dialled.
#[derive(Debug, Clone, Default)]
pub struct ConnectionCounter(Arc<AtomicU64>);

impl ConnectionCounter {
    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Exponential backoff between retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub first_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: u32,
}

impl RetryPolicy {
    /// The delay before retry number `attempt` (1-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
        self.first_delay.saturating_mul(factor).min(self.max_delay)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            first_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            max_attempts: 8,
        }
    }
}
