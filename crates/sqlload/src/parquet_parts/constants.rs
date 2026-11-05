/// A file is finished once the writer's estimate reaches this many bytes (12 MB, which counts buffered rows before compression: about 5 MB on disk for wide rows). A `COPY` reads its files in parallel, and on an X-Small warehouse 4 files of 5 MB loaded about 4 times faster than one of 20 MB, and 8 of 24 MB twice as fast as 2 of 75 MB (live, 2026-11-05).
pub const PART_FILE_BYTES: usize = 12 * 1000 * 1000;

/// Rows written to a file at a time, so a file ends near its size whatever the batches' size.
pub const PART_SLICE_ROWS: usize = 8_192;

/// A row group is flushed once its buffered data reaches this many bytes (128 MiB).
pub const ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// The most rows in one row group.
pub const ROW_GROUP_ROWS: usize = 1_000_000;
