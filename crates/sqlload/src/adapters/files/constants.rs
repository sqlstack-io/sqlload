/// A file is written out once its rows reach this many.
pub const FILE_MAX_ROWS: usize = 1_000_000;

/// A file is written out once its encoded bytes reach about this many (128 MiB).
pub const FILE_MAX_BYTES: usize = 128 * 1024 * 1024;

/// The largest file is written out while all open files together hold more than this (512 MiB).
pub const OPEN_FILES_MAX_BYTES: usize = 512 * 1024 * 1024;

/// The name every written file starts with, followed by the load id and a part number.
pub const PART_PREFIX: &str = "part-";

/// The file name suffix of the Parquet files written.
pub const PARQUET_SUFFIX: &str = ".parquet";

/// The placeholder for the cursor's value in a path template.
pub const CURSOR_PLACEHOLDER: &str = "{cursor}";

/// The placeholder for the cursor's date in a path template.
pub const CURSOR_DATE_PLACEHOLDER: &str = "{cursor:date}";

/// What every cursor placeholder starts with.
pub const CURSOR_PLACEHOLDER_START: &str = "{cursor";

/// The time of day partition bounds of timestamp cursors are written with.
pub const MIDNIGHT: &str = "00:00:00.000000";

/// The offset zoned timestamp bounds end with.
pub const UTC_SUFFIX: &str = "+00";

/// Seconds in a day, to turn Parquet date statistics (days since 1970) into dates.
pub const SECONDS_PER_DAY: i64 = 86_400;

/// The folder below a table's folder holding one marker per load that has not finished.
pub const PENDING_FOLDER: &str = "_sqlload/pending/";

/// Marks the hidden folder a filesystem `replace` stages the table in, beside the table folder: `.<table folder>.sqlload-<load id>`.
pub const STAGED_FOLDER_MARK: &str = ".sqlload-";

/// The first line of a pending marker while the load writes its files.
pub const PENDING_WRITING: &str = "writing";

/// The first line of a pending marker while the load deletes the files it replaces.
pub const PENDING_REPLACING: &str = "replacing";

/// At most this many partitions have a file open at once; beyond it the largest is written out.
pub const OPEN_FILES_MAX: usize = 256;

/// Memory an open file holds beyond its buffered data (writer state, page buffers), for the memory estimate.
pub const OPEN_FILE_OVERHEAD_BYTES: usize = 1024 * 1024;

/// The start of a pending marker line holding a run record (keys never start with `@`).
pub const PENDING_RUN_PREFIX: &str = "@run\t";

/// The file name suffix of the CSV files written, before any compression suffix.
pub const CSV_SUFFIX: &str = ".csv";

/// The suffix of gzip-compressed CSV files.
pub const GZIP_SUFFIX: &str = ".gz";

/// The suffix of zstd-compressed CSV files.
pub const ZSTD_SUFFIX: &str = ".zst";

/// The name suffixes of a files table's data files, whatever format wrote them, so a replace removes files of an earlier format too.
pub const DATA_SUFFIXES: &[&str] = &[".parquet", ".csv", ".csv.gz", ".csv.zst"];

/// CSV text compressed into one zstd frame once this many bytes wait (4 MiB).
pub const ZSTD_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// The file name of a `single_file` table whose folder has no name of its own.
pub const SINGLE_FILE_STEM: &str = "data";

/// How timestamps with a zone are written to CSV: RFC 3339 in UTC.
pub const CSV_TIMESTAMP_TZ: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";

/// How timestamps without a zone are written to CSV.
pub const CSV_TIMESTAMP: &str = "%Y-%m-%dT%H:%M:%S%.6f";

/// How dates are written to CSV.
pub const CSV_DATE: &str = "%Y-%m-%d";

/// How times of day are written to CSV.
pub const CSV_TIME: &str = "%H:%M:%S%.6f";
