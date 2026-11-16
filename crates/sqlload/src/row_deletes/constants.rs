/// The largest percentage of a table's rows a key scan marks deleted when `checks.max_row_drop` sets none, so an empty or narrowed source read never marks the whole table.
pub const DEFAULT_MAX_DROP: u8 = 5;
