/// The source kind as written, shown in `plan`.
pub const ODBC_SYSTEM: &str = "odbc";

/// The table types discovery lists.
pub const TABLE_TYPES: &str = "TABLE,VIEW";

/// The search pattern matching every schema and table name.
pub const ANY_NAME: &str = "%";

/// The identifier quote SQLLoad writes, ANSI SQL's.
pub const IDENTIFIER_QUOTE: char = '"';

/// A query returning one row only when the database reads `"name"` as a name rather than a string.
pub const ANSI_QUOTES_PROBE: &str =
    "SELECT 1 FROM (SELECT 1 AS sqlload_probe) sqlload_query WHERE \"sqlload_probe\" = 1";

/// The driver's answer when it does not quote identifiers.
pub const NO_QUOTE: char = ' ';

/// The alias a query table's rows are read under.
pub const QUERY_ALIAS: &str = "sqlload_query";

/// Batches read ahead of the one being loaded.
pub const BATCHES_AHEAD: usize = 2;

/// The buffer per value over which a text or binary column is read value by value, unless the source sets `long_column_threshold`: 4 KiB.
pub const DEFAULT_LONG_COLUMN_THRESHOLD: usize = 4 * 1024;

/// The most memory one fetch of a table holds, unless the source sets `max_batch_memory` or the table's share of the memory budget is too small: 512 MiB.
pub const DEFAULT_BATCH_MEMORY: usize = 512 * 1024 * 1024;

/// The text buffer arrow-odbc binds for a time in milliseconds, microseconds and nanoseconds (`HH:MM:SS.fff` and longer).
pub const TIME_TEXT_LENGTHS: [usize; 3] = [12, 15, 18];

/// The buffer a column read value by value keeps between values: 64 KiB. Each read fills the whole buffer first, so a longer value's buffer is given back after it.
pub const KEPT_VALUE_BUFFER: usize = 64 * 1024;

/// The bytes a value read value by value holds besides its own: a text or binary value's offset, and about as much for a fixed-size value's validity and padding.
pub const VALUE_OVERHEAD: usize = 4;

/// How many of a table's long columns `plan` names.
pub const NAMED_LONG_COLUMNS: usize = 3;

/// What the driver manager's error says when a value is longer than its buffer.
pub const TOO_LARGE_VALUE: &str = "too large to be written into the allocated buffer";

/// The fractional digits a timestamp cursor bound is described with: seven, so SQL Server compares it as `datetime2(7)`, exactly, rather than as a `datetime`.
pub const TIMESTAMP_PARAMETER_PRECISION: i16 = 7;

/// The name Microsoft's SQL Server drivers report for the database.
pub const SQL_SERVER_DBMS: &str = "Microsoft SQL Server";

/// The fractional digits SQL Server's driver describes a legacy `datetime` parameter with.
pub const LEGACY_DATETIME_DIGITS: std::ops::RangeInclusive<i16> = 1..=3;

/// The fractional digits of a SQL Server timestamp bound sent as text when the driver can't describe its parameter: `datetime2`'s seven, which a legacy `datetime` refuses rather than rounds.
pub const TIMESTAMP_TEXT_DIGITS: u16 = 7;

/// The primary key and unique constraints the database's `INFORMATION_SCHEMA` lists for one table (schema, then name, bound), a constraint's columns in order: what a table's driver should have reported as its key.
pub const LISTED_KEYS_QUERY: &str = "SELECT tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME, kcu.COLUMN_NAME \
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME \
WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE') AND tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? \
ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION";
