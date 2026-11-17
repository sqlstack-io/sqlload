/// Set to `1` to rewrite a page's output blocks from the run instead of comparing them.
pub const UPDATE_VARIABLE: &str = "SQLLOAD_UPDATE_DOCS";

/// A line of an output block standing for the lines the page leaves out.
pub const ELISION: &str = "…";

/// What differs from run to run in command output, and what it is compared as.
pub const VOLATILE: &[(&str, &str)] = &[
    (r"\b\d+\.\d+s\b", "_s"),
    (r"\d+(\.\d+)? [KMG]iB", "_ MiB"),
    (r"PostgreSQL [\d.]+ \([^)]*\)", "PostgreSQL _"),
    (r"DuckDB v[\d.]+", "DuckDB v_"),
    (r"\s+", " "),
];

/// The Postgres image a page's `docker run` starts.
pub const PAGE_POSTGRES_IMAGE: (&str, &str) = ("postgres", "17");

/// Connection attempts while the page's Postgres finishes starting, half a second apart.
pub const CONNECT_ATTEMPTS: usize = 60;

/// The line that opens and closes a fenced block, before an opening line's language.
pub const FENCE: &str = "```";

/// How a section's heading starts.
pub const HEADING: &str = "## ";

/// The project file a page writes or a command writes for it.
pub const PROJECT_FILE: &str = "sqlload.toml";

/// The host the page's project names for its Postgres, rewritten to the test server's.
pub const PAGE_HOST_LINE: &str = "host = \"localhost\"";
