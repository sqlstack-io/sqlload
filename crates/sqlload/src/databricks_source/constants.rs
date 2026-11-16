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
