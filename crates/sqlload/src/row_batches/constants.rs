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

/// The start of a projection's path: the row object itself.
pub const PROJECTION_ROOT: &str = "$";

/// Separates the keys of a projection's path, `$.address.city`.
pub const PROJECTION_SEPARATOR: char = '.';

/// How many of a flattened object's keys `plan` shows.
pub const SHOWN_OBJECT_KEYS: usize = 2;

/// More keys than this make a flattened object look like a map whose keys are data.
pub const MANY_KEYS: usize = 20;

/// The percentage of a flattened object's keys that, holding a digit or not a plain name, make it look like a map whose keys are data.
pub const DATA_KEYS_SHARE: usize = 50;
