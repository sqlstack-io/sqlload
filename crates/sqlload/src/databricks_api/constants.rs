use std::time::Duration;

/// Where OAuth machine-to-machine tokens come from.
pub const TOKEN_PATH: &str = "/oidc/v1/token";

/// The Statement Execution API.
pub const STATEMENTS_PATH: &str = "/api/2.0/sql/statements";

/// The Files API for files, and for directories, in volumes.
pub const FILES_PATH: &str = "/api/2.0/fs/files";
pub const DIRECTORIES_PATH: &str = "/api/2.0/fs/directories";

/// How long a statement request waits for its result before it is polled instead.
pub const WAIT_TIMEOUT: &str = "30s";

/// Between polls of a running statement.
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// A token is fetched again this long before it expires.
pub const TOKEN_REFRESH_MARGIN_MS: i64 = 120_000;

/// The HTTP status a request with an expired token gets.
pub const UNAUTHORIZED: u16 = 401;

/// The HTTP status of a missing file or directory.
pub const NOT_FOUND: u16 = 404;
