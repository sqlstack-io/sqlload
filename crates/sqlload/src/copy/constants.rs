/// The start of the source's and target's names, `copy_<table>`.
pub const COPY_PREFIX: &str = "copy_";

/// Where a literal password in the source URI is kept, so the project text only references it.
pub const SOURCE_PASSWORD_VARIABLE: &str = "SQLLOAD_COPY_SOURCE_PASSWORD";

/// Where a literal password in the destination URI is kept.
pub const DESTINATION_PASSWORD_VARIABLE: &str = "SQLLOAD_COPY_DESTINATION_PASSWORD";

/// The first line of the project `sqlload copy --print-toml` prints.
pub const PROJECT_HEADER: &str = "# Written by `sqlload copy --print-toml`; append it to sqlload.toml (it adds a target, a source and any URI connections) to load this table on every run.\n\n";

/// What a password shows as.
pub const REDACTED: &str = "***";

/// The query settings an `s3://` URI takes.
pub const S3_SETTINGS: &[&str] = &["region", "endpoint"];

/// The query settings a `postgresql://` URI takes, as libpq names them.
pub const POSTGRES_SETTINGS: &[&str] = &["sslmode", "sslrootcert", "sslcert", "sslkey"];
