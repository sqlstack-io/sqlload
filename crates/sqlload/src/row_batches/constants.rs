/// The UTF-8 byte order mark some JSON bytes start with.
pub const BYTE_ORDER_MARK: &[u8] = b"\xEF\xBB\xBF";

/// The time zone timestamps with an offset are carried in.
pub const UTC_ZONE: &str = "UTC";

/// The length of the shortest RFC 3339 timestamp, `YYYY-MM-DDTHH:MM:SSZ`.
pub const MIN_TIMESTAMP_LENGTH: usize = 20;
