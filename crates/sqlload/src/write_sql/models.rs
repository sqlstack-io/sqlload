use crate::column_types::models::Warehouse;
use crate::schema_plan::models::DesiredColumn;

/// A destination table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    pub database: Option<String>,
    pub schema: String,
    pub table: String,
}

/// How staged rows are written into the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteMode {
    Append,
    /// Upsert: rows whose key matches a staged row are replaced.
    Merge {
        unique_key: Vec<String>,
    },
    /// Rows with `cursor_column >= from` (all rows when `from` is `None`) are replaced by the staged rows.
    DeleteInsert {
        cursor_column: String,
        from: Option<String>,
    },
    /// The staged rows become the whole table, atomically.
    Replace,
}

/// Everything the statements of one write need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteStatementsRequest<'a> {
    pub warehouse: Warehouse,
    pub table: &'a TableName,
    /// The columns each row carries, `_sqlload_*` metadata included.
    pub columns: &'a [DesiredColumn],
    pub mode: &'a WriteMode,
    pub load_id: &'a str,
    /// The load time as an ISO 8601 UTC timestamp.
    pub loaded_at: &'a str,
}
