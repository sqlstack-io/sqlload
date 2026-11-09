/// The system name a BigQuery source's tables carry in plans.
pub const BIGQUERY_SOURCE: &str = "bigquery";

/// The most streams a read session is split into; BigQuery may give fewer.
pub const READ_STREAMS: i32 = 8;

/// How many batches the streams read ahead of the consumer.
pub const BATCHES_AHEAD: usize = 8;

/// How many tables' metadata discovery reads at once.
pub const TABLES_AT_ONCE: usize = 8;

/// The table types the Storage Read API reads directly; views and others are read through a query.
pub const READ_API_TABLES: &[&str] = &["TABLE"];

/// The precision and scale of a `NUMERIC` column declared without them.
pub const NUMERIC_DEFAULT: (u8, u8) = (38, 9);
