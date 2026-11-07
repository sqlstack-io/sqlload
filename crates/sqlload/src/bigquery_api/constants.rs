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

/// The Storage Write API's root.
pub const STORAGE_URL: &str = "https://bigquerystorage.googleapis.com";

/// The Storage Write API's methods, as gRPC paths.
pub const CREATE_WRITE_STREAM: &str =
    "/google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream";
pub const APPEND_ROWS: &str = "/google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows";
pub const FINALIZE_WRITE_STREAM: &str =
    "/google.cloud.bigquery.storage.v1.BigQueryWrite/FinalizeWriteStream";
pub const BATCH_COMMIT_WRITE_STREAMS: &str =
    "/google.cloud.bigquery.storage.v1.BigQueryWrite/BatchCommitWriteStreams";

/// The metadata Google's front end routes a Storage Write API call by.
pub const REQUEST_PARAMS: &str = "x-goog-request-params";

/// `WriteStream.Type.PENDING`: rows stay invisible until the stream is committed.
pub const PENDING_STREAM: i32 = 2;

/// How many append requests one stream holds before its sender waits for BigQuery to take them.
pub const APPENDS_IN_FLIGHT: usize = 2;

/// The reason Google gives a request refused for exceeding a rate limit, with HTTP 403.
pub const RATE_LIMITED: &str = "rateLimitExceeded";

/// How long opening a Storage Write API connection may take.
pub const GRPC_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// How often a Storage Write API connection with calls open is pinged, so a silently dropped one is noticed (no pings while idle, which Google's servers count against a client).
pub const GRPC_KEEP_ALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a keep-alive ping may go unanswered before the connection and its calls fail.
pub const GRPC_KEEP_ALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// How long a unary Storage Write API call (create, finalize, commit) may take.
pub const GRPC_UNARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
