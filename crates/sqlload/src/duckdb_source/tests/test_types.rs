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
