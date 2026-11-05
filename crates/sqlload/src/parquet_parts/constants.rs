/// A file is finished once its encoded bytes reach about this many (6 MB): a `COPY` reads its files in parallel, and on an X-Small warehouse 4 to 8 files of this size loaded 3 to 4 times faster than one or two large ones (live, 2026-11-05).
pub const PART_FILE_BYTES: usize = 6 * 1000 * 1000;

/// Rows written to a file at a time, so a file ends near its size whatever the batches' size.
pub const PART_SLICE_ROWS: usize = 8_192;

/// A row group is flushed once its buffered data reaches this many bytes (128 MiB).
pub const ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// The most rows in one row group.
pub const ROW_GROUP_ROWS: usize = 1_000_000;
