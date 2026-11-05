/// A file is finished once its encoded bytes reach about this many (100 MB).
pub const PART_FILE_BYTES: usize = 100 * 1000 * 1000;

/// A row group is flushed once its buffered data reaches this many bytes (128 MiB).
pub const ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// The most rows in one row group.
pub const ROW_GROUP_ROWS: usize = 1_000_000;
