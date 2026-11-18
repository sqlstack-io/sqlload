/// The system name a DuckDB source's tables carry in plans.
pub const DUCKDB_SOURCE: &str = "duckdb";

/// The database path DuckDB opens in memory, which holds no tables to read.
pub const MEMORY_DATABASE: &str = ":memory:";

/// What `DESCRIBE` writes in its `null` column for a column that is never NULL.
pub const NOT_NULL: &str = "NO";

/// How many result batches the read holds ahead of the consumer.
pub const BATCHES_AHEAD: usize = 4;
