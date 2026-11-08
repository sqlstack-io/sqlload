pub const IMAGE: &str = "clickhouse/clickhouse-server";
/// The oldest server SQLLoad's ClickHouse target is tested against.
pub const TAG: &str = "25.8";
pub const USER: &str = "sqlload";
pub const PASSWORD: &str = "sqlload";
pub const HTTP_PORT: u16 = 8123;
/// Query attempts while the server finishes starting.
pub const READY_ATTEMPTS: usize = 120;
pub const READY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
