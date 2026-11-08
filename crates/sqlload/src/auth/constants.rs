pub const PROGRAMMATIC_ACCESS_TOKEN: &str = "PROGRAMMATIC_ACCESS_TOKEN";
pub const KEYPAIR_JWT: &str = "KEYPAIR_JWT";
/// Snowflake accepts key-pair JWTs for at most one hour.
pub const JWT_LIFETIME_SECS: u64 = 3540;
pub const JWT_REFRESH_MARGIN_SECS: u64 = 300;
pub const OAUTH: &str = "OAUTH";
/// The client of Snowflake's built-in `SNOWFLAKE$LOCAL_APPLICATION` integration; its secret is the same.
pub const LOCAL_APPLICATION: &str = "LOCAL_APPLICATION";
/// Refresh an OAuth access token this long before it expires.
pub const OAUTH_REFRESH_MARGIN_SECS: u64 = 60;
/// The token cache, below the user's cache folder.
pub const TOKEN_CACHE_FILE: &str = "sqlload/snowflake-tokens.json";
/// What the browser shows when Snowflake sent back an error instead of a code.
pub const SIGN_IN_FAILED_PAGE: &str = "<!doctype html><title>SQLLoad</title><p>The Snowflake sign-in failed. Return to the terminal for the reason.</p>";
/// How long the browser sign-in may take before SQLLoad stops waiting.
pub const SIGN_IN_TIMEOUT_SECS: u64 = 300;
/// The most of a redirect request SQLLoad reads.
pub const REDIRECT_REQUEST_LIMIT: usize = 16 * 1024;
/// What the browser shows once the sign-in reached SQLLoad.
pub const SIGNED_IN_PAGE: &str = "<!doctype html><title>SQLLoad</title><p>Signed in to Snowflake. You can close this tab and return to the terminal.</p>";
