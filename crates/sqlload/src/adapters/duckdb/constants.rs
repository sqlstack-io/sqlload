/// Columns of one table, in table order.
pub const DESCRIBE_TABLE_SQL: &str = "SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
/// A table's `CREATE TABLE` statement, by schema and name.
pub const DEFINITION_QUERY: &str = "SELECT sql FROM duckdb_tables() WHERE database_name = current_database() AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?)";
/// The `CREATE INDEX` statements of a table's indexes, by schema and name.
pub const INDEXES_QUERY: &str = "SELECT sql FROM duckdb_indexes() WHERE database_name = current_database() AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?) AND sql IS NOT NULL ORDER BY index_name";
