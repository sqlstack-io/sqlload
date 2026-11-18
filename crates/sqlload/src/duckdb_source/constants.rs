/// The system name a DuckDB source's tables carry in plans.
pub const DUCKDB_SOURCE: &str = "duckdb";

/// The system name a MotherDuck source's tables carry in plans.
pub const MOTHERDUCK_SOURCE: &str = "motherduck";

/// The database path DuckDB opens in memory, which holds no tables to read.
pub const MEMORY_DATABASE: &str = ":memory:";

/// What `DESCRIBE` writes in its `null` column for a column that is never NULL.
pub const NOT_NULL: &str = "NO";

/// How many result batches the read holds ahead of the consumer.
pub const BATCHES_AHEAD: usize = 4;

/// How DuckDB writes `true` cast to `VARCHAR`.
pub const TRUE_TEXT: &str = "true";

/// The catalog queries, every value cast to `VARCHAR` so a file and the MotherDuck bridge answer alike, all of the opened database's own: its tables and views (schema, name, comment, estimated rows), their columns in order (schema, table, name, type, nullable), and their primary keys (schema, table, one key column a row, in key order). They use only DuckDB's own functions, none of an extension it would have to download first.
pub const CATALOG_QUERIES: [&str; 3] = [
    "SELECT schema_name, table_name, comment, CAST(estimated_size AS VARCHAR) FROM duckdb_tables() WHERE database_name = current_database() \
     UNION ALL SELECT schema_name, view_name, comment, '-1' FROM duckdb_views() WHERE database_name = current_database() AND NOT internal",
    "SELECT schema_name, table_name, column_name, data_type, CAST(is_nullable AS VARCHAR) FROM duckdb_columns() WHERE database_name = current_database() ORDER BY schema_name, table_name, column_index",
    "SELECT schema_name, table_name, column_name FROM (\
     SELECT schema_name, table_name, unnest(constraint_column_names) AS column_name, generate_subscripts(constraint_column_names, 1) AS position \
     FROM duckdb_constraints() WHERE database_name = current_database() AND constraint_type = 'PRIMARY KEY'\
     ) ORDER BY schema_name, table_name, position",
];
