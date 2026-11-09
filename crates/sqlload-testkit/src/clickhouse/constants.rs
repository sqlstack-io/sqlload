pub const IMAGE: &str = "clickhouse/clickhouse-server";
/// The oldest server SQLLoad's ClickHouse target is tested against.
pub const TAG: &str = "25.8";
/// The variable naming another server tag to test against, such as a 26.x release.
pub const TAG_VARIABLE: &str = "SQLLOAD_TEST_CLICKHOUSE_TAG";
pub const USER: &str = "sqlload";
pub const PASSWORD: &str = "sqlload";
pub const HTTP_PORT: u16 = 8123;
/// Query attempts while the server finishes starting.
pub const READY_ATTEMPTS: usize = 120;
pub const READY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
