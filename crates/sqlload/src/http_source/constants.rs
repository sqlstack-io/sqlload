use std::time::Duration;

/// The delay before the first retry; each later one doubles it.
pub const FIRST_RETRY_DELAY: Duration = Duration::from_millis(500);

/// The longest backoff between retries.
pub const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// The longest wait a `Retry-After` header is obeyed for.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// Characters of a failed response's body shown in its error.
pub const ERROR_EXCERPT_CHARS: usize = 300;

/// Pages fetched ahead of the parser before a job waits.
pub const PAGES_AHEAD: usize = 4;

/// Threads of the runtime one job's requests run on.
pub const JOB_THREADS: usize = 2;

/// The `Link` header parameter of the next page, spaces removed.
pub const NEXT_RELATION: &str = "rel=\"next\"";
