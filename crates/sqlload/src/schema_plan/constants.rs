/// A merge's staged rows numbered in source order, so the last of those sharing a key is kept; staging only, never the table.
pub const MERGE_ROW_COLUMN: &str = "_sqlload_row";

/// The command-line flag that allows table rebuilds for one run.
pub const ALLOW_REBUILD_FLAG: &str = "--allow-type-change-rebuild";

/// The prefix of the columns captured changes add.
pub const CDC_PREFIX: &str = "_sqlload_";

/// Captured changes: the commit LSN of the source transaction that wrote the row's values.
pub const CDC_LSN_COLUMN: &str = "_sqlload_lsn";

/// Captured changes: what the newest change to the row was (`snapshot`, `insert`, `update`, `delete` or `truncate`).
pub const CDC_OP_COLUMN: &str = "_sqlload_op";

/// Captured changes: when the source transaction committed (NULL for snapshot rows).
pub const CDC_COMMITTED_AT_COLUMN: &str = "_sqlload_committed_at";

/// Captured changes with soft deletes: when the row was deleted in the source, NULL while it exists.
pub const CDC_DELETED_AT_COLUMN: &str = "_sqlload_deleted_at";

/// Captured changes in a changelog: the change's position within its transaction.
pub const CDC_SEQ_COLUMN: &str = "_sqlload_seq";

/// Captured changes: the data columns whose unchanged TOAST value the source left out, as `,a,b,` (NULL when none was).
pub const CDC_UNCHANGED_COLUMN: &str = "_sqlload_unchanged";
