use crate::column_types::models::ColumnType;

/// The largest percentage of a table's rows a key scan marks deleted when `checks.max_row_drop` sets none, so an empty or narrowed source read never marks the whole table.
pub const DEFAULT_MAX_DROP: u8 = 5;

/// The tombstone cursor types a soft delete's mark (`deletes_from`) takes its time from.
pub const TOMBSTONE_TIMES: [ColumnType; 2] = [ColumnType::TimestampNtz, ColumnType::TimestampTz];
