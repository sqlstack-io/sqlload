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

/// One `SHOW` listing: its columns and rows.
pub(crate) type Listing = (&'static [&'static str], Vec<Vec<Option<&'static str>>>);

/// A kept table as `SCHEMA.NAME`, its row and byte estimates, and its comment.
pub(crate) type ListedTable = (&'static str, i64, i64, Option<&'static str>);

pub(crate) struct ListedTablesTestCase {
    pub(crate) description: &'static str,
    /// The `SHOW TABLES` and `SHOW VIEWS` results.
    pub(crate) listings: Vec<Listing>,
    pub(crate) include: &'static [&'static str],
    pub(crate) exclude: &'static [&'static str],
    /// The kept tables in order; `None` when the listing is not trusted.
    pub(crate) expected_tables: Option<Vec<ListedTable>>,
}

pub(crate) struct DescribedColumnsTestCase {
    pub(crate) description: &'static str,
    /// `DESCRIBE TABLE` rows: name, type and `null?`.
    pub(crate) rows: &'static [[&'static str; 3]],
    /// Each column's loaded name, nullability and type.
    pub(crate) expected_columns: Vec<(&'static str, bool, Result<ColumnType, String>)>,
}
