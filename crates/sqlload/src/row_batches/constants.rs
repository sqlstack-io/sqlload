/// Joins a flattened object's name and its key: `customer` and `country` give `customer__country`.
pub const FLATTEN_SEPARATOR: &str = "__";

/// The UTF-8 byte order mark some JSON bytes start with.
pub const BYTE_ORDER_MARK: &[u8] = b"\xEF\xBB\xBF";

/// The time zone timestamps with an offset are carried in.
pub const UTC_ZONE: &str = "UTC";

/// The length of the shortest RFC 3339 timestamp, `YYYY-MM-DDTHH:MM:SSZ`.
pub const MIN_TIMESTAMP_LENGTH: usize = 20;

/// The digits before the point a decimal needs to hold any 64-bit integer.
pub const INT_DIGITS: u8 = 19;

/// Seconds in a day, for dates held as days since 1970-01-01.
pub const SECONDS_PER_DAY: i64 = 86_400;

/// The schema metadata key listing the columns flattening made in a batch, joined by `FLATTENED_SEPARATOR`.
pub const FLATTENED_METADATA_KEY: &str = "sqlload.flattened";

/// Separates the names under `FLATTENED_METADATA_KEY`.
pub const FLATTENED_SEPARATOR: &str = "\u{0}";
