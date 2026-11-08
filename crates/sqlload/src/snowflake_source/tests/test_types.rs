use crate::column_types::models::ColumnType;

pub(crate) struct NaturalTypeTestCase {
    pub(crate) description: &'static str,
    /// The type as Snowflake prints it.
    pub(crate) type_text: &'static str,
    /// The portable type, or the error.
    pub(crate) expected_type: Result<ColumnType, &'static str>,
}

pub(crate) struct UnloadExpressionTestCase {
    pub(crate) description: &'static str,
    pub(crate) type_text: &'static str,
    pub(crate) output: ColumnType,
    pub(crate) expected_expression: Option<&'static str>,
}

pub(crate) struct LoadedNameTestCase {
    pub(crate) description: &'static str,
    pub(crate) name: &'static str,
    pub(crate) expected_name: &'static str,
}

pub(crate) struct UnloadQueryTestCase {
    pub(crate) description: &'static str,
    /// The query table's SQL; `None` reads the table.
    pub(crate) sql: Option<&'static str>,
    pub(crate) filter: Option<&'static str>,
    /// Pinned types, by column name.
    pub(crate) pinned: &'static [(&'static str, ColumnType)],
    /// The unload `SELECT`, or the problems with the columns.
    pub(crate) expected_query: Result<&'static str, Vec<&'static str>>,
}

pub(crate) struct ConvertBatchTestCase {
    pub(crate) description: &'static str,
    /// The unloaded Parquet decimal's precision and scale.
    pub(crate) unloaded: (u8, i8),
    pub(crate) values: &'static [Option<i128>],
    pub(crate) output: ColumnType,
    /// Each value as text (NULL as empty), or the error.
    pub(crate) expected_values: Result<Vec<&'static str>, &'static str>,
}

pub(crate) struct RowsUnloadedTestCase {
    pub(crate) description: &'static str,
    /// The `COPY INTO` result's columns and first row.
    pub(crate) columns: &'static [&'static str],
    pub(crate) row: &'static [Option<&'static str>],
    /// The row count, or the error.
    pub(crate) expected_rows: Result<u64, &'static str>,
}
