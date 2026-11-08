/// The source kind as written, shown in `plan`.
pub const ODBC_SYSTEM: &str = "odbc";

/// The table types discovery lists.
pub const TABLE_TYPES: &str = "TABLE,VIEW";

/// The search pattern matching every schema and table name.
pub const ANY_NAME: &str = "%";

/// The identifier quote SQLLoad writes, ANSI SQL's.
pub const IDENTIFIER_QUOTE: char = '"';

/// The driver's answer when it does not quote identifiers.
pub const NO_QUOTE: char = ' ';

/// The alias a query table's rows are read under.
pub const QUERY_ALIAS: &str = "sqlload_query";

/// Batches read ahead of the one being loaded.
pub const BATCHES_AHEAD: usize = 2;

/// What the driver manager's error says when a value is longer than its buffer.
pub const TOO_LARGE_VALUE: &str = "too large to be written into the allocated buffer";
