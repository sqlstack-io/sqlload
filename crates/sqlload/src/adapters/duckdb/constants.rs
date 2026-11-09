/// Columns of one table in the current database (MotherDuck attaches the account's other databases too), in table order.
pub const DESCRIBE_TABLE_SQL: &str = "SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length FROM information_schema.columns WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ? ORDER BY ordinal_position";
/// A table's `CREATE TABLE` statement, by schema and name.
pub const DEFINITION_QUERY: &str = "SELECT sql FROM duckdb_tables() WHERE database_name = current_database() AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?)";
/// The `CREATE INDEX` statements of a table's indexes, by schema and name.
pub const INDEXES_QUERY: &str = "SELECT sql FROM duckdb_indexes() WHERE database_name = current_database() AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?) AND sql IS NOT NULL ORDER BY index_name";
/// The UTC offset an unloaded timestamp with time zone's text is read with.
pub const UTC_OFFSET: &str = "+00";
/// The environment variables a MotherDuck token is taken from when the connection sets none, in order.
pub const MOTHERDUCK_TOKEN_VARIABLES: &[&str] = &["motherduck_token", "MOTHERDUCK_TOKEN"];
/// Part of the message MotherDuck's extension refuses to load with on a DuckDB version it does not support.
pub const MOTHERDUCK_UNSUPPORTED_DUCKDB: &str = "is not yet supported by MotherDuck";
/// What precedes the extension's own message in DuckDB's error.
pub const EXTENSION_EXCEPTION: &str = "threw an exception: ";
