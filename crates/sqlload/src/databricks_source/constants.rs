/// The system name a Databricks source's tables carry in plans.
pub const DATABRICKS_SOURCE: &str = "databricks";

/// How many result files a read downloads at once.
pub const LINKS_AT_ONCE: usize = 8;

/// How many decoded batches the downloads hold ahead of the consumer.
pub const BATCHES_AHEAD: usize = 8;

/// How many times one result file is downloaded, each retry through a fresh link, as a link expires within 15 minutes.
pub const DOWNLOAD_ATTEMPTS: usize = 3;

/// The schema every catalog has, which discovery skips.
pub const INFORMATION_SCHEMA: &str = "information_schema";

/// The precision and scale of a `DECIMAL` declared without them.
pub const DECIMAL_DEFAULT: (u32, u32) = (10, 0);

/// What `information_schema.columns.is_nullable` says of a column that can't hold NULL.
pub const NOT_NULLABLE: &str = "NO";

/// The most fractional-second digits a timestamp loads with; a `TIMESTAMP(9)` would lose its nanoseconds.
pub const TIMESTAMP_DIGITS: u32 = 6;

/// How a column type names its collation, and the byte-order one cursor filters compare text in.
pub const COLLATE: &str = " COLLATE ";
pub const BINARY_COLLATION: &str = "UTF8_BINARY";
