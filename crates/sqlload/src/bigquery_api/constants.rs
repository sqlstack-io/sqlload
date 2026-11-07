/// The BigQuery REST API's root.
pub const BASE_URL: &str = "https://bigquery.googleapis.com/bigquery/v2";

/// The OAuth scope a service account asks for.
pub const SCOPE: &str = "https://www.googleapis.com/auth/bigquery";

/// The OAuth grant that exchanges a signed service-account assertion for a token.
pub const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// How long a signed assertion is valid; Google takes at most an hour.
pub const ASSERTION_LIFETIME_SECS: u64 = 3600;

/// A token is fetched again this long before it expires.
pub const TOKEN_REFRESH_MARGIN_MS: i64 = 120_000;

/// How long a query or results request waits on the server before it answers that the job still runs.
pub const QUERY_WAIT_MS: u64 = 30_000;

/// The HTTP status a request with an expired token gets.
pub const UNAUTHORIZED: u16 = 401;

/// The HTTP status of a missing table or dataset.
pub const NOT_FOUND: u16 = 404;
