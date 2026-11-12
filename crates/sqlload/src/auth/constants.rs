pub const PROGRAMMATIC_ACCESS_TOKEN: &str = "PROGRAMMATIC_ACCESS_TOKEN";
pub const KEYPAIR_JWT: &str = "KEYPAIR_JWT";
/// Snowflake accepts key-pair JWTs for at most one hour.
pub const JWT_LIFETIME_SECS: u64 = 3540;
pub const JWT_REFRESH_MARGIN_SECS: u64 = 300;
pub const OAUTH: &str = "OAUTH";
/// The client of Snowflake's built-in `SNOWFLAKE$LOCAL_APPLICATION` integration; its secret is the same.
pub const LOCAL_APPLICATION: &str = "LOCAL_APPLICATION";
/// Refresh an OAuth access token this long before it expires (half its lifetime when that is shorter).
pub const OAUTH_REFRESH_MARGIN_SECS: u64 = 60;
/// How long Snowflake's access tokens last unless the integration says otherwise.
pub const DEFAULT_ACCESS_LIFETIME_SECS: u64 = 600;
/// The token cache, below the user's cache folder.
pub const TOKEN_CACHE_FILE: &str = "sqlload/snowflake-tokens.json";
/// What the browser shows when Snowflake sent back an error instead of a code.
pub const SIGN_IN_FAILED_PAGE: &str = "<!doctype html><title>SQLLoad</title><p>The {provider} sign-in failed. Return to the terminal for the reason.</p>";
/// Where the browser sign-in redirects to: SQLLoad listens there.
pub const LOOPBACK_HOST: &str = "127.0.0.1";
pub const LOOPBACK_SCHEME: &str = "http";
/// How long the browser sign-in may take before SQLLoad stops waiting.
pub const SIGN_IN_TIMEOUT_SECS: u64 = 300;
/// How long one connection to the sign-in listener may take to send its request.
pub const REDIRECT_READ_TIMEOUT_SECS: u64 = 10;
/// How often, and how many milliseconds apart, SQLLoad checks whether the browser opener failed before taking the browser as opened.
pub const BROWSER_CHECKS: u32 = 20;
pub const BROWSER_CHECK_MILLIS: u64 = 100;
/// What a request to the sign-in listener that is not this sign-in's redirect gets.
pub const NOT_THIS_SIGN_IN_PAGE: &str = "<!doctype html><title>SQLLoad</title><p>This is not the {provider} sign-in SQLLoad is waiting for.</p>";
/// The most of a redirect request SQLLoad reads.
pub const REDIRECT_REQUEST_LIMIT: usize = 16 * 1024;
/// What the browser shows once the sign-in reached SQLLoad.
pub const SIGNED_IN_PAGE: &str = "<!doctype html><title>SQLLoad</title><p>Signed in to {provider}. You can close this tab and return to the terminal.</p>";
/// The name in the pages above.
pub const PROVIDER_PLACEHOLDER: &str = "{provider}";
/// The provider of Snowflake sign-ins, as messages name it.
pub const SNOWFLAKE_PROVIDER: &str = "Snowflake";
