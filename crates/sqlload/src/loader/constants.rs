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

/// The route of a table a Snowflake or BigQuery target selects from a source on its own connection, after the warehouse's name.
pub const SELECTED_ROUTE: &str =
    "INSERT ... SELECT on the source's connection (nothing through the loader)";

/// How a skipped files source's reason ends after `no files to load in <path>`, when `after_load` emptied its folder.
pub const DRAINED_REASON: &str = ": after_load deleted or moved the ones already loaded";

/// The label of a plan detail saying how a table's rows travel, which `plan` shows directly under `source`.
pub const ROUTE_LABEL: &str = "route";

/// The label of a plan detail saying where a table's rows come from.
pub const SOURCE_LABEL: &str = "source";

/// The label of an error's detail saying what is wrong.
pub const REASON_LABEL: &str = "reason";

/// The label of an error's detail saying how to fix it.
pub const FIX_LABEL: &str = "fix";

/// The comparison reading a cursor's lower bound itself.
pub const INCLUSIVE_BOUND: &str = ">=";

/// The comparison reading only values after a cursor's lower bound.
pub const EXCLUSIVE_BOUND: &str = ">";

/// A condition no row meets, so a database checks a query without scanning the table.
pub const NO_ROWS: &str = "1 = 0";

/// The source type label Python columns carry.
pub const PYTHON_COLUMN_TYPE: &str = "python";

/// Why the standalone executable cannot run a project's Python sources.
pub const NO_PYTHON_HOST: &str = "this project has Python sources (python_paths), which run only in the sqlload Python package's command; install it (for example `uv tool install sqlload`) and run its sqlload instead of the standalone executable";

/// Why the standalone executable cannot run a `transform`; `{place}` names the source or table that sets it.
pub const NO_TRANSFORM_HOST: &str = "{place} sets transform, which runs only in the sqlload Python package's command; install it (for example `uv tool install sqlload`) and run its sqlload instead of the standalone executable";

/// Why the standalone executable cannot run a `rename` function; `{place}` names the source or table that sets it.
pub const NO_RENAME_HOST: &str = "{place} sets rename to a function, which runs only in the sqlload Python package's command; install it (for example `uv tool install sqlload`) and run its sqlload, or give rename a map";

/// The rows of a file or range a `transform` returned dicts for, from which a slow call gets the hint that Arrow or a DataFrame is faster.
pub const DICT_HINT_ROWS: u64 = 100_000;

/// How long a `transform` that returned dicts took, in milliseconds, from which a large file's call gets the hint that Arrow or a DataFrame is faster.
pub const DICT_HINT_MILLISECONDS: u64 = 5_000;

/// The cursor type a Python table function is told before the table's first load shows it.
pub const UNKNOWN_CURSOR_TYPE: &str = "unknown";

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

/// The least time between two `table_progress` events of a batch still being read.
pub const PROGRESS_EVERY: std::time::Duration = std::time::Duration::from_millis(500);

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
/// What is running now: the live block's badge, bar and spinners.
pub const CYAN: &str = "\x1b[36m";
/// Swaps text and background colour: the live block's `LOADING` badge, a cyan block.
pub const INVERSE: &str = "\x1b[7m";
/// Back to plain text.
pub const RESET: &str = "\x1b[0m";

/// The host name that always means this machine: plain text to it never crosses a network.
pub const LOCALHOST: &str = "localhost";

/// Why Snowflake does not read a Snowflake source's unload itself when no external stage of the target covers it.
pub const UNLOADED_OUTSIDE_STAGE: &str = "Snowflake reads another connection's unload itself only through the target's external stage: set the source's stage to an external one (url and storage_integration) whose url is inside the target's external stage url";
/// Why a warehouse source's unloaded files go through SQLLoad rather than straight into the destination.
pub const UNLOADED_TIMES: &str = "the warehouse writes times and timestamps as whole microseconds (its Parquet writer keeps only milliseconds), which SQLLoad turns back into them";

/// Why an `odbc` source cannot be read by this build.
pub const NO_ODBC: &str = "this build of SQLLoad has no ODBC support; build it with the odbc feature (cargo install sqlload --features odbc)";

/// Why a build without DuckDB refuses a `duckdb` source.
pub const NO_DUCKDB_SOURCE: &str = "this build of SQLLoad has no DuckDB support; build it with the duckdb feature (cargo install sqlload --features duckdb)";

/// The plan warning for a files table whose checks pull its files through SQLLoad instead of the destination's own read.
pub const CHECKS_LOADER_WARNING: &str = "checks run on rows SQLLoad stages, so every file goes through the loader rather than the destination's own read, which is slower; without checks the table stays on the direct route.";

/// The `system` of a Snowflake source's tables.
pub const SNOWFLAKE_SOURCE: &str = "snowflake";

/// The `system` of an ODBC source's tables.
pub const ODBC_SOURCE: &str = "odbc";

/// The constraint type a database's `INFORMATION_SCHEMA` gives a primary key.
pub const PRIMARY_KEY: &str = "PRIMARY KEY";

/// The `unique_key` that stands for the primary key a table's source reports.
pub const PRIMARY_KEY_PLACEHOLDER: &str = "{primary_key}";

/// The version of the document `apply --json` and `load --json` print; it changes only when a field changes meaning or goes away.
pub const RESULT_SCHEMA_VERSION: u32 = 1;

/// The plan's note on a `constant_columns` column.
pub const CONSTANT_NOTE: &str = "  (constant)";

/// The end of a merge's plan line when a key column may hold NULL, which the merge matches with a NULL.
pub const NULL_KEYS_NOTE: &str = "; NULLs in keys match";

/// The most `replace_key` values `load --json` lists for a table; it says when there were more.
pub const REPLACED_KEYS_SHOWN: usize = 1000;

/// The SQLSTATEs of Postgres errors worth retrying unchanged: a connection that failed or dropped (not 08P01, which PgBouncer also sends for a database it does not know), out of memory, too many connections, a server shutting down or starting, a serialization failure, a deadlock and a lock not available.
pub const TRANSIENT_SQLSTATES: [&str; 11] = [
    "08000", "08003", "08006", "53200", "53300", "57P01", "57P02", "57P03", "40001", "40P01",
    "55P03",
];

/// HTTP statuses a rerun may get past whatever the service's code says: throttling, a bad gateway, unavailable, a gateway timeout.
pub const TRANSIENT_STATUSES: [u16; 4] = [429, 502, 503, 504];

/// Service codes that make an HTTP 500 worth retrying: ClickHouse's timeouts, socket and network errors, too many queries or parts, and memory limits; BigQuery's backend and internal errors. A 500 with any other code (ClickHouse answers a bad query with one) is not.
pub const TRANSIENT_SERVER_CODES: [&str; 9] = [
    "159",
    "202",
    "209",
    "210",
    "241",
    "252",
    "backendError",
    "internalError",
    "rateLimitExceeded",
];

/// The `unnest` word that unnests every list field.
pub const UNNEST_ALL: &str = "all";

/// How many dropped rows' ordinals one `DELETE` from a child table's staging table lists.
pub const DROPPED_CHUNK: usize = 1000;

/// The length of a date written as `YYYY-MM-DD`; a longer `--start-cursor-ts` value has a time of day.
pub const DATE_TEXT_LENGTH: usize = 10;

/// Why a cursor range can't apply to a table without a cursor.
pub const RANGE_NEEDS_CURSOR: &str =
    "--start-cursor/--end-cursor and their -ts and -int forms need a cursor_column";

/// The note of a `transform` record whose load rewrote the table whole, so the files earlier records name are gone from it.
pub const TRANSFORM_REWROTE_NOTE: &str = "transform, table rewritten";

/// How many windows' rows outside them the load report shows a line for; later ones are counted in one line.
pub const SHOWN_OUTSIDE_WINDOWS: usize = 3;

/// What a tombstone table's name adds to the name of the table whose `deletes_from` it is.
pub const TOMBSTONE_SUFFIX: &str = "__sqlload_deletes";

/// Why `--sync-deletes` fails a plan none of whose selected tables sets `deletes`.
pub const NO_DELETES_TABLES: &str = "--sync-deletes reads every key of each selected table with deletes and marks its rows of the keys the source no longer holds, but none of the selected tables sets deletes. Fix: set deletes = \"soft\" or \"hard\" on a merge table, or drop --sync-deletes";

/// Rows a merge [published once per run](crate::loader::_helpers::publish::publishes_once) stages before it commits anyway, at its next cut: matching the staged keys holds them all in one set on the server (about 90 bytes a key on ClickHouse), which this keeps under about 1 GiB.
pub const MERGE_RUN_ROWS: u64 = 10_000_000;

/// How `plan` words a run published in one commit.
pub const PUBLISHED_ONCE: &str = "published in one commit";

/// How `plan` words a `delete_insert` published in one commit.
pub const RANGE_PUBLISHED_ONCE: &str = "published in one commit replacing the whole range";

/// How `plan` words a merge published in one commit, up to [`MERGE_RUN_ROWS`] rows.
pub const MERGE_PUBLISHED_ONCE: &str = "published in one commit, or one every 10 million rows";

/// Part of the error DuckDB refuses a `CREATE TABLE IF NOT EXISTS` with while another connection's create of the same table has not committed yet.
pub const CATALOG_CONFLICT: &str = "Catalog write-write conflict";

/// How often a table's create refused for [`CATALOG_CONFLICT`] is tried, 20 ms apart: the other connection's create commits within moments.
pub const CATALOG_CONFLICT_TRIES: u32 = 10;
