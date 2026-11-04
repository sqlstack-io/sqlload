pub const IMAGE: &str = "postgres";
pub const TAG: &str = "17-alpine";
pub const USER: &str = "sqlload";
pub const PASSWORD: &str = "sqlload";
pub const DATABASE: &str = "app";
/// Connection attempts while the server finishes starting.
pub const CONNECT_ATTEMPTS: usize = 60;
pub const CONNECT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
