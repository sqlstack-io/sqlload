use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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

/// Caps on uploads and on loads in flight at once, shared by every stream, and the time spent waiting for a slot.
#[derive(Debug, Clone)]
pub struct InFlight {
    uploads: Arc<Semaphore>,
    loads: Arc<Semaphore>,
    waited_ms: Arc<AtomicU64>,
}

impl InFlight {
    pub fn new(limit: usize) -> Self {
        Self {
            uploads: Arc::new(Semaphore::new(limit.max(1))),
            loads: Arc::new(Semaphore::new(limit.max(1))),
            waited_ms: Arc::default(),
        }
    }

    /// A slot for one file upload.
    pub async fn upload(&self) -> Option<OwnedSemaphorePermit> {
        self.acquire(&self.uploads).await
    }

    /// A slot for one load request (insertFiles or COPY).
    pub async fn load(&self) -> Option<OwnedSemaphorePermit> {
        self.acquire(&self.loads).await
    }

    /// Milliseconds spent waiting for slots since start.
    pub fn waited_ms(&self) -> u64 {
        self.waited_ms.load(Ordering::Relaxed)
    }

    async fn acquire(&self, semaphore: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
        let started = Instant::now();
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(permit) => Some(permit),
            Err(_) => None,
        };
        let waited = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.waited_ms.fetch_add(waited, Ordering::Relaxed);
        permit
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
