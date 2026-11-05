use std::time::Duration;

use crate::column_types::models::ColumnType;
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
    /// Rows with `from <= cursor_column < to` (open-ended when `None`) are replaced by the staged rows; with `values`, the rows whose cursor is one of them (a files source's partitions) instead.
    DeleteInsert {
        cursor_column: String,
        from: Option<String>,
        to: Option<String>,
        values: Vec<String>,
    },
    /// The staged rows become the whole table, atomically.
    Replace,
    /// The table is rebuilt from the staged rows with the current columns and types (`--reload`), in one statement.
    Recreate,
}

/// Everything the statements of one write need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteStatementsRequest<'a> {
    pub table: &'a TableName,
    /// The columns each row carries, `_sqlload_*` metadata included.
    pub columns: &'a [DesiredColumn],
    pub mode: &'a WriteMode,
    /// Columns staged in plain text that the write hashes in SQL (`hash` masking on the direct route): the lowercase hex SHA-256 of their text.
    pub hashed: &'a [String],
    /// The batches went straight into `staging`, built in the table's shape: the write only swaps it in, so no row is written twice, then re-creates these indexes.
    pub built: Option<&'a [String]>,
}

/// What a table rebuilt beside itself must keep, where the warehouse cannot copy it (DuckDB).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableShape {
    /// The `(...)` of its `CREATE TABLE`: columns and constraints.
    pub definition: String,
    /// The statements creating its indexes.
    pub indexes: Vec<String>,
}

/// A cursor position to read from the destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorQuery<'a> {
    pub table: &'a TableName,
    pub column: &'a str,
    pub column_type: &'a ColumnType,
    pub lookback: Option<Duration>,
}

/// How a cursor query's single value is turned into the lower bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorValue {
    /// Microseconds since the Unix epoch; `zoned` instants print with `+00`, wall-clock times without.
    EpochMicros { zoned: bool },
    /// An ISO `YYYY-MM-DD` date.
    Date,
    /// Used as it is (integers, decimals, text).
    Plain,
}

/// A query for a cursor's maximum and how to read its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorRead {
    pub sql: String,
    pub value: CursorValue,
}

/// Where one table's batches go, as the load planned them.
#[derive(Debug, Clone, Copy)]
pub struct WriteTarget<'a> {
    /// The table itself.
    pub table: &'a TableName,
    /// The staging or build table the batches land in, when they do not go straight into the table.
    pub staging: Option<&'a TableName>,
    /// The columns staged batches carry, the `_sqlload_*` metadata included.
    pub columns: &'a [DesiredColumn],
    pub load_id: &'a str,
    /// The batch being filled, from 1.
    pub batch: u64,
    /// Batches go straight into the table, each committing.
    pub direct: bool,
    /// The batches land in a build table in their final types.
    pub builds: bool,
}
