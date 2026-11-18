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
/// MotherDuck's server version, such as `v1.5.6-2026-11-382`.
pub const MOTHERDUCK_VERSION_QUERY: &str = "SELECT md_version()";
/// The first DuckDB major version that shreds a VARIANT column's common fields into columns, so queries on them are fast.
pub const SHREDDING_DUCKDB_MAJOR: u32 = 2;
/// Writes what DuckDB holds since its last checkpoint to its file.
pub const CHECKPOINT_STATEMENT: &str = "CHECKPOINT";
/// Part of the error a `CHECKPOINT` is refused with while another connection's open transaction updates rows or changes the catalog (appends and deletes don't block it). `FORCE CHECKPOINT` would wait instead, but blocks every other connection from beginning a transaction meanwhile, which can deadlock the load's tables.
pub const CHECKPOINT_BUSY: &str = "other write transactions active";
/// How long a refused `CHECKPOINT` waits before it tries again; the transactions that refuse it are single commits, which end within moments.
pub const CHECKPOINT_RETRY: std::time::Duration = std::time::Duration::from_millis(20);
/// How long a refused `CHECKPOINT` keeps trying before it is left for later: what it would write stays in the write-ahead log, which the next checkpoint, or the next load opening the database, writes.
pub const CHECKPOINT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
/// The settings a DuckDB target opens with under a memory budget, before its `memory_limit` and `threads`: no automatic checkpoint, whose running out of memory would make the database unusable; SQLLoad checkpoints after each piece's commit as needed and after each table instead.
pub const NO_AUTOCHECKPOINT: &str = "SET checkpoint_threshold = '1TB'";
/// The suffix of a DuckDB file's write-ahead log.
pub const WAL_SUFFIX: &str = ".wal";
