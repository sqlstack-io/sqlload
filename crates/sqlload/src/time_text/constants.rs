/// Months 0..10 of a March-based year are March to December.
pub const MARCH_BASED_YEAR_END: i64 = 10;
pub const FEBRUARY: u32 = 2;

/// The length of a timestamp to the second, as `2026-11-14 07:49:41`, which `plan` cuts a cursor's time to.
pub const SECOND_TIMESTAMP_LENGTH: usize = 19;

/// The length of an offset of whole hours, as `+00`.
pub const HOURS_OFFSET_LENGTH: usize = 3;
