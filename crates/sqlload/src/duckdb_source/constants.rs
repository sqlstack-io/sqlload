/// The system name a DuckDB source's tables carry in plans.
pub const DUCKDB_SOURCE: &str = "duckdb";

/// How many result batches the read holds ahead of the consumer.
pub const BATCHES_AHEAD: usize = 4;
