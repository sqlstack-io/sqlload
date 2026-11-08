/// The most digits of a `NUMBER(p,0)` loaded as a 64-bit integer.
pub const LONGEST_INT_DIGITS: u8 = 18;

/// Text columns at least this long (Snowflake's default `VARCHAR` length) load as unbounded text.
pub const LONGEST_VARCHAR: u32 = 16_777_216;

/// Midnight, which `TIME` values are counted from in microseconds.
pub const TIME_ZERO: &str = "'00:00:00'::TIME";

/// The type times and timestamps unload as: whole microseconds.
pub const UNLOAD_INTEGER: &str = "NUMBER(18,0)";

/// The user stage folder unloads go to when the source sets no `stage`.
pub const USER_STAGE_FOLDER: &str = "@~/sqlload_unload/";

/// Unloaded files downloaded at once.
pub const PARALLEL_DOWNLOADS: usize = 8;

/// The session schema; tables are always named with their schema.
pub const SESSION_SCHEMA: &str = "PUBLIC";

/// Every table and view column of the session's database, with each table's size and comment.
pub const CATALOG_SQL: &str = "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.numeric_precision, c.numeric_scale, c.character_maximum_length, c.is_nullable, t.row_count, t.bytes, t.comment \
FROM information_schema.columns c JOIN information_schema.tables t \
ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
WHERE c.table_schema <> 'INFORMATION_SCHEMA' \
ORDER BY c.table_schema, c.table_name, c.ordinal_position";

/// `information_schema.columns.is_nullable` for a column that cannot be NULL.
pub const CATALOG_NOT_NULL: &str = "NO";

/// `DESCRIBE RESULT`'s `null?` for a column that cannot be NULL.
pub const DESCRIBED_NOT_NULL: &str = "N";
