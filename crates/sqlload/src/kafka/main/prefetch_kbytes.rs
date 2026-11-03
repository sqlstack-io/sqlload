//! Split the prefetch budget across consumers.

use crate::kafka::constants::{MAX_PREFETCH_KBYTES, MIN_PREFETCH_KBYTES};

/// One consumer's share of `budget_bytes` when `consumers` share it, in librdkafka's kilobytes.
pub fn prefetch_kbytes(budget_bytes: u64, consumers: usize) -> u64 {
    let consumers = u64::try_from(consumers.max(1)).unwrap_or(u64::MAX);
    (budget_bytes / 1024 / consumers).clamp(MIN_PREFETCH_KBYTES, MAX_PREFETCH_KBYTES)
}
