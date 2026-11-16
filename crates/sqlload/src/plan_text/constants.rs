//! What the plan's default lines leave out, and how they are laid out.

/// Details a table's line or its source's line already says, or that only restate how every table of the kind loads: `-v` shows them.
pub const QUIET_LABELS: [&str; 8] = [
    "source", "reads", "commits", "memory", "send", "fetch", "buffer", "key",
];

/// The start of the `schema` line of a Python table whose columns are inferred, as every Python table's are.
pub const INFERRED_SCHEMA: &str = "columns inferred ";

/// Words of a `route` that make it a table's own: one where no row passes through SQLLoad, or files copied unchanged.
pub const OWN_ROUTES: [&str; 2] = ["(nothing through the loader)", "uploaded unchanged"];

/// The label of a route detail.
pub const ROUTE: &str = "route";

/// The label of a `schema` detail.
pub const SCHEMA: &str = "schema";

/// The label of a commit size detail, which a files table's files decide.
pub const BATCH: &str = "batch";

/// The label of a files table's count of files to read.
pub const FILES: &str = "files";

/// The column the command a section's heading names starts at: `Loads (15)  into DuckDB warehouse` then `sqlload load`.
pub const HINT_COLUMN: usize = 80;

/// The width the `Skipped` lines wrap their table names at.
pub const SKIPPED_WIDTH: usize = 100;

/// The action of a table `apply` creates, whose columns the default plan counts rather than lists.
pub const CREATE_TABLE: &str = "create table";

/// The start of the action of a table `--reload` rebuilds, whose columns are counted too.
pub const RECREATE_TABLE: &str = "recreate table";

/// The starts of an `Objects` detail that names a column left out or masked, which the default plan keeps.
pub const NAMED_COLUMNS: [&str; 2] = ["excluded  ", "masked  "];

/// The start of the line naming a new table's `_sqlload_*` columns, separated by `, `.
pub const LOAD_COLUMNS: &str = "+ ";

/// What a skipped files table's reason starts with when no file is new, and the group it joins.
pub const NO_NEW_FILES: (&str, &str) = ("no new files in ", "no new files");

/// What a skipped files table's reason starts with when every file was read, and the group it joins.
pub const UP_TO_DATE: (&str, &str) = ("up to date: 0 new of ", "up to date");

/// What a skipped files table's reason starts with when a range holds no file, and the group it joins.
pub const NOTHING_IN_RANGE: (&str, &str) = ("nothing in range: ", "nothing in range");

/// The bytes of a MiB, and the MiB of a GiB, for the `max_batch_memory` a fix suggests.
pub const MIB: (u64, u64) = (1_048_576, 1024);

/// Where the bound of a read starting at the warehouse's maximum is said, and the `lookback` after it.
pub const MAXIMUM: (&str, &str) = (" (warehouse maximum", " minus lookback ");
