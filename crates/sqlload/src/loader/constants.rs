/// Rows per batch read from a database source.
pub const SOURCE_BATCH_ROWS: usize = 65_536;

/// The column where `plan` details start.
pub const PLAN_DETAIL_COLUMN: usize = 52;

/// The comparison reading a cursor's lower bound itself.
pub const INCLUSIVE_BOUND: &str = ">=";

/// The comparison reading only values after a cursor's lower bound.
pub const EXCLUSIVE_BOUND: &str = ">";

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

/// Why a Python table with an enforced contract needs declared columns.
pub const ENFORCED_UNDECLARED_PROBLEM: &str = "contract enforced needs declared columns: the first load would create a table with only metadata columns";

/// The most tables loading at once when neither `concurrency` nor `--concurrency` says.
pub const DEFAULT_THREADS: usize = 4;

/// The JSON a table function's `ctx.state` holds when it holds nothing.
pub const NO_STATE: &str = "null";

/// Starts the one snapshot a Postgres table's cursor windows are all read in.
pub const SNAPSHOT_BEGIN: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

/// Blocking threads beyond one per table loading at once, for the reads and encoding those tables wait on.
pub const SPARE_BLOCKING_THREADS: usize = 64;

/// Postgres connections reading one large table at once, when its read is whole and unordered.
pub const PARALLEL_READERS: usize = 2;

/// Tables Postgres estimates below this size (128 MiB) are read over one connection.
pub const PARALLEL_READ_MIN_BYTES: i64 = 128 * 1024 * 1024;
