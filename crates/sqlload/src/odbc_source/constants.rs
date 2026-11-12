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

/// What the driver manager's error says when a value is longer than its buffer.
pub const TOO_LARGE_VALUE: &str = "too large to be written into the allocated buffer";

/// The fractional digits a timestamp cursor bound is described with: seven, so SQL Server compares it as `datetime2(7)`, exactly, rather than as a `datetime`.
pub const TIMESTAMP_PARAMETER_PRECISION: i16 = 7;

/// The name Microsoft's SQL Server drivers report for the database.
pub const SQL_SERVER_DBMS: &str = "Microsoft SQL Server";

/// The fractional digits SQL Server's driver describes a legacy `datetime` parameter with.
pub const LEGACY_DATETIME_DIGITS: std::ops::RangeInclusive<i16> = 1..=3;
