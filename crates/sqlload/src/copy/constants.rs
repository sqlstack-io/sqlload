/// The source and the target of the project `sqlload copy` builds.
pub const COPY_NAME: &str = "copy";

/// The connection a source URI becomes.
pub const SOURCE_CONNECTION: &str = "copy_source";

/// The connection a destination URI becomes.
pub const DESTINATION_CONNECTION: &str = "copy_destination";

/// Where a literal password in the source URI is kept, so the project text only references it.
pub const SOURCE_PASSWORD_VARIABLE: &str = "SQLLOAD_COPY_SOURCE_PASSWORD";

/// Where a literal password in the destination URI is kept.
pub const DESTINATION_PASSWORD_VARIABLE: &str = "SQLLOAD_COPY_DESTINATION_PASSWORD";

/// The first line of the project `sqlload copy --print-toml` prints.
pub const PROJECT_HEADER: &str = "# Written by `sqlload copy --print-toml`; add it to sqlload.toml to load this table on every run.\n\n";

/// What a password shows as.
pub const REDACTED: &str = "***";

/// The query settings an `s3://` URI takes.
pub const S3_SETTINGS: &[&str] = &["region", "endpoint"];
