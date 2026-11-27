/// The BigQuery REST API's root.
pub const BASE_URL: &str = "https://bigquery.googleapis.com/bigquery/v2";

/// The OAuth scope a service account asks for.
pub const SCOPE: &str = "https://www.googleapis.com/auth/bigquery";

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

/// The Storage Read API's methods, as gRPC paths.
pub const CREATE_READ_SESSION: &str =
    "/google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession";
pub const READ_ROWS: &str = "/google.cloud.bigquery.storage.v1.BigQueryRead/ReadRows";

/// `DataFormat.ARROW`.
pub const ARROW_FORMAT: i32 = 2;

/// `ArrowSerializationOptions.CompressionCodec.LZ4_FRAME`.
pub const LZ4_FRAME: i32 = 1;

/// How many times a read stream broken by a retryable error is resumed where it stopped.
pub const READ_RESUMES: u32 = 3;

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

/// The OAuth scope Cloud Storage uploads ask for.
pub const STORAGE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// Cloud Storage's upload root.
pub const STORAGE_UPLOAD_URL: &str = "https://storage.googleapis.com/upload/storage/v1";

/// The HTTP status of a job id already taken.
pub const CONFLICT: u16 = 409;

/// The first and longest wait between reads of a running load job.
pub const JOB_POLL_FIRST: std::time::Duration = std::time::Duration::from_millis(500);
pub const JOB_POLL_MAX: std::time::Duration = std::time::Duration::from_secs(5);
