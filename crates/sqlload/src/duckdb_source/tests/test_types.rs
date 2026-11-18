use crate::column_types::models::ColumnType;

pub(crate) struct SourceColumnTestCase {
    pub(crate) description: &'static str,
    /// The type as `duckdb_columns().data_type` or a result's column type writes it.
    pub(crate) data_type: &'static str,
    /// The portable type; `None` when a type must be pinned.
    pub(crate) expected_type: Option<ColumnType>,
}

/// Which tables a shop database's catalog lists for a source's settings.
pub(crate) struct ReadCatalogTestCase {
    pub(crate) description: &'static str,
    /// Lines of the `[sources.shop]` table: `include`, `exclude` and the like.
    pub(crate) source_lines: &'static str,
    /// The tables listed, by qualified name, in order.
    pub(crate) expected_tables: &'static [&'static str],
}

/// The primary key the catalog reports for one table, in key order.
pub(crate) struct PrimaryKeyTestCase {
    pub(crate) description: &'static str,
    /// The table's qualified name as the catalog lists it.
    pub(crate) table: &'static str,
    pub(crate) expected_key: &'static [&'static str],
}

/// One read of a table or query table the shop source lists.
pub(crate) struct ReadRowsTestCase {
    pub(crate) description: &'static str,
    /// The table's qualified name as the catalog lists it.
    pub(crate) table: &'static str,
    /// A DuckDB condition on the read columns.
    pub(crate) filter: Option<&'static str>,
    pub(crate) batch_rows: usize,
    /// Each named column's values in row order as Arrow displays them (`""` for NULL), as `name=value|value`.
    pub(crate) expected_values: &'static [&'static str],
}

/// A duckdb connection whose database the source cannot read.
pub(crate) struct RefusedReaderTestCase {
    pub(crate) description: &'static str,
    pub(crate) database: &'static str,
    /// Part of the refusal.
    pub(crate) expected_error: &'static str,
}

/// Reads of a database file open in this process, some of them ended, while another process tries to write the file.
pub(crate) struct LockedFileTestCase {
    pub(crate) description: &'static str,
    pub(crate) reads: usize,
    /// How many of the reads have ended when the other process writes.
    pub(crate) ended: usize,
    /// What the other process says.
    pub(crate) expected_said: &'static str,
}

/// Reads of one database file open in this process, some of them ended, each with a 128 MiB `memory_limit`, and the limit their shared instance has.
pub(crate) struct SharedLimitTestCase {
    pub(crate) description: &'static str,
    pub(crate) reads: usize,
    pub(crate) ended: usize,
    /// `current_setting('memory_limit')` as DuckDB prints it.
    pub(crate) expected_limit: &'static str,
}
