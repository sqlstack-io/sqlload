use std::time::Duration;

/// The delay before the first retry; each later one doubles it.
pub const FIRST_RETRY_DELAY: Duration = Duration::from_millis(500);

/// The longest backoff between retries.
pub const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// The longest wait a `Retry-After` header is obeyed for.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// The most time before an OAuth2 access token's expiry at which it is renewed; a short-lived token is renewed a tenth of its lifetime early.
pub const MAX_TOKEN_RENEWAL_MARGIN: Duration = Duration::from_secs(60);

/// The longest lifetime an OAuth2 access token is taken to have, whatever its `expires_in` says.
pub const MAX_TOKEN_LIFETIME: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// How old an OAuth2 access token must be before a 401 that `on_status` skips renews it: younger tokens are taken as valid and the 401 as the API's answer, so a run of skipped 401s costs at most one token request per this long.
pub const SKIPPED_REFUSAL_RENEWAL_AGE: Duration = Duration::from_secs(30);

/// Characters of a failed response's body shown in its error.
pub const ERROR_EXCERPT_CHARS: usize = 300;

/// Pages fetched ahead of the parser before a job waits.
pub const PAGES_AHEAD: usize = 4;

/// Threads of the runtime one job's requests run on.
pub const JOB_THREADS: usize = 2;

/// The `Link` header parameter naming relations.
pub const RELATION_PARAMETER: &str = "rel";

/// Redirects followed within the connection's origin before a request fails.
pub const MAX_REDIRECTS: usize = 10;

/// Redirects a client follows by resending a `POST` as a `GET` without its body.
pub const BODY_DROPPING_REDIRECTS: [u16; 3] = [301, 302, 303];

/// The `Link` relation of the next page.
pub const NEXT_RELATION: &str = "next";

/// Path segments that would climb the URL's path if a fan-out key filled one unencoded.
pub const DOT_SEGMENTS: &[&str] = &[".", ".."];

/// Words in a header's name that mark its value as a secret to redact from errors.
pub const SECRET_HEADER_WORDS: &[&str] = &[
    "auth",
    "token",
    "key",
    "secret",
    "cookie",
    "session",
    "signature",
    "pass",
    "credential",
    "jwt",
    "bearer",
];

/// Key values a failed batch's error shows before it gives only their count.
pub const SHOWN_KEYS: usize = 5;

/// The most pages `read_ahead` keeps requested at once.
pub const MAX_READ_AHEAD: u64 = 100;
