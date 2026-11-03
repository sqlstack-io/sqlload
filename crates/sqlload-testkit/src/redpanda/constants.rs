pub const IMAGE: &str = "redpandadata/redpanda";
pub const TAG: &str = "v25.1.4";
/// Reads of committed offsets while the group coordinator is not ready.
pub const OFFSET_ATTEMPTS: usize = 20;
pub const OFFSET_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
