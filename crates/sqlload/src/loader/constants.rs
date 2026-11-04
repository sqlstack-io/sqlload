/// Rows per batch read from a database source.
pub const SOURCE_BATCH_ROWS: usize = 65_536;

/// The column where `plan` details start.
pub const PLAN_DETAIL_COLUMN: usize = 52;

/// The comparison reading a cursor's lower bound itself.
pub const INCLUSIVE_BOUND: &str = ">=";

/// The comparison reading only values after a cursor's lower bound.
pub const EXCLUSIVE_BOUND: &str = ">";

/// Arrow bytes of yielded Python data held in memory before they are written to a temporary file as one chunk; replayed chunks become the staged batches.
pub const SPILL_CHUNK_BYTES: usize = 128 * 1024 * 1024;

/// The source type label Python columns carry.
pub const PYTHON_COLUMN_TYPE: &str = "python";

/// Why the standalone executable cannot run a project's Python sources.
pub const NO_PYTHON_HOST: &str = "this project has Python sources (python_paths), which run only in the sqlload Python package's command; install it (for example `uv tool install sqlload`) and run its sqlload instead of the standalone executable";

/// The cursor type a Python table function is told before the table's first load shows it.
pub const UNKNOWN_CURSOR_TYPE: &str = "unknown";

/// The offset suffix of cursor bounds in UTC, as `cursor_text` writes them.
pub const UTC_OFFSET_SUFFIX: &str = "+00";

/// The remark for a Python table's first load that yielded no rows and declares no columns, so nothing was created.
pub const NO_DATA_NOTE: &str = "(table created on first data)";

/// Why an empty Python load fails under `replace`; `{table}` is the table's name.
pub const EMPTY_REPLACE_PROBLEM: &str = "yielded no rows; replace would empty {table}; set allow_empty=True on the table if an empty result is real";
