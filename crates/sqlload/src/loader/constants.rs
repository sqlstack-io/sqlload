/// Why `--reload` is refused where SQLLoad never drops or recreates tables.
pub const KEPT_TABLES_RELOAD_PROBLEM: &str = "--reload drops and recreates the table, which SQLLoad never does on a Postgres target; write_strategy = \"replace\" empties and refills it in one transaction instead";

/// How to add a column a load needs to a table that has rows, where `--reload` would rebuild it.
pub const RELOAD_FIX: &str = "run it once with --reload";

/// The same on a Postgres target, which never drops and recreates a table.
pub const KEPT_TABLES_COLUMN_FIX: &str = "SQLLoad never drops and recreates a Postgres table (--reload), so add the column with ALTER TABLE, then empty the table or fill the column, and load again";

/// Rows per batch read from a database source.
pub const SOURCE_BATCH_ROWS: usize = 65_536;

/// The column where `plan` details start.
pub const PLAN_DETAIL_COLUMN: usize = 52;

/// The width of a plan detail's label and the space after it, as `route   `.
pub const LABEL_WIDTH: usize = 8;

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

/// ANSI escapes for coloured output.
pub const BOLD: &str = "\x1b[1m";
/// Dimmed text: a heading's command note.
pub const DIM: &str = "\x1b[2m";
/// Errors and failures.
pub const RED: &str = "\x1b[38;5;167m";
/// A complete plan or load.
pub const GREEN: &str = "\x1b[32m";
/// Warnings.
pub const YELLOW: &str = "\x1b[33m";
/// Back to plain text.
pub const RESET: &str = "\x1b[0m";

/// The host name that always means this machine: plain text to it never crosses a network.
pub const LOCALHOST: &str = "localhost";

/// Why a warehouse source's unloaded files go through SQLLoad rather than straight into the destination.
pub const UNLOADED_TIMES: &str = "the warehouse writes times and timestamps as whole microseconds (its Parquet writer keeps only milliseconds), which SQLLoad turns back into them";

/// Why an `odbc` source cannot be read by this build.
pub const NO_ODBC: &str = "this build of SQLLoad has no ODBC support; build it with the odbc feature (cargo install sqlload --features odbc)";

/// The plan warning for a files table whose checks pull its files through SQLLoad instead of the destination's own read.
pub const CHECKS_LOADER_WARNING: &str = "checks run on rows SQLLoad stages, so every file goes through the loader rather than the destination's own read, which is slower; without checks the table stays on the direct route.";

/// The `system` of a Snowflake source's tables.
pub const SNOWFLAKE_SOURCE: &str = "snowflake";

/// The version of the document `apply --json` and `load --json` print; it changes only when a field changes meaning or goes away.
pub const RESULT_SCHEMA_VERSION: u32 = 1;
