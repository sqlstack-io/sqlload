use std::time::Duration;

use crate::column_types::models::ColumnType;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::{BatchFooters, DataFile, FilesRead};
use crate::loader::models::TableSpec;
use crate::postgres_source::models::ReadColumn;
use crate::schema_plan::models::{DesiredColumn, LiveColumn};
use crate::snowflake_source::models::UnloadedFiles;

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
    /// Captured changes (`kind = "postgres_cdc"`), applied only where their `_sqlload_lsn` is newer than what the table holds, so a replayed batch changes nothing.
    Changes(ChangeWrite),
}

/// How a batch of captured changes is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeWrite {
    pub apply: ChangeApply,
    /// The source's key columns, which `merge` matches rows on.
    pub unique_key: Vec<String>,
    /// The newest truncate in the batch, applied before its rows (`merge` only; a changelog keeps it as a row).
    pub truncated: Option<Truncation>,
}

/// What a captured change does to the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeApply {
    /// One row per key; a delete sets `_sqlload_deleted_at`.
    SoftDeletes,
    /// One row per key; a delete removes the row.
    HardDeletes,
    /// Every change appended as a row.
    Changelog,
}

/// A source `TRUNCATE`: its transaction's commit LSN and time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncation {
    pub lsn: i64,
    /// RFC 3339 UTC.
    pub committed_at: String,
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

/// Which way a cursor bound counted from now moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowShift {
    /// Back by `lookback`, for the window's start.
    Back(Duration),
    /// Ahead by `lookahead`, for the window's end.
    Ahead(Duration),
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

/// A cursor bound passed to a `?` placeholder rather than written into the filter, typed as its cursor column compares it, so the database's literal rules can't change it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundParameter {
    /// A text cursor's bound.
    Text(String),
    /// A wall-clock timestamp cursor's bound.
    Timestamp(chrono::NaiveDateTime),
    /// A date cursor's bound.
    Date(chrono::NaiveDate),
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
    /// The table's own columns in their final types, which a build table has.
    pub write_columns: &'a [DesiredColumn],
    pub load_id: &'a str,
    /// The batch being filled, from 1.
    pub batch: u64,
    /// Batches go straight into the table, each committing.
    pub direct: bool,
    /// The batches land in a build table in their final types.
    pub builds: bool,
}

/// What a direct batch's load did (the destination reading the files itself), with the rows it loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectLoaded {
    /// Committed with the load.
    Committed(u64),
    /// In the staging table, waiting for the commit.
    Staged(u64),
}

/// One files batch the destination may load straight from the source's own files.
#[derive(Debug, Clone, Copy)]
pub struct DirectBatch<'a> {
    pub spec: &'a TableSpec,
    /// The files table's planned read.
    pub read: &'a FilesRead,
    /// The table's columns as the load writes them.
    pub write_columns: &'a [DesiredColumn],
    pub target: WriteTarget<'a>,
    /// The load time as an ISO 8601 UTC timestamp, written to every row.
    pub loaded_at: &'a str,
    pub files: &'a [DataFile],
    pub footers: &'a BatchFooters,
    /// The write mode of the batch's commit.
    pub mode: &'a WriteMode,
    /// The live table's columns as plan read them.
    pub live: Option<&'a [LiveColumn]>,
    /// The files' own folder, which `copy_to_internal_stage` uploads them from.
    pub source_folder: Option<&'a OpenFolder>,
}

/// One unload's files the destination loads by reading them itself: times and timestamps held as whole microseconds, every other column as the type it is read as.
#[derive(Debug, Clone, Copy)]
pub struct UnloadedBatch<'a> {
    pub files: &'a UnloadedFiles,
    /// The columns the files hold, as they are read.
    pub read: &'a [ReadColumn],
    /// The table's columns as the load writes them.
    pub write_columns: &'a [DesiredColumn],
    pub target: WriteTarget<'a>,
    /// The load time as an ISO 8601 UTC timestamp, written to every row.
    pub loaded_at: &'a str,
}

/// A query a check before publish runs on a relation; each returns its values as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckQuery<'a> {
    /// One text value: how many rows the relation holds, then how many of them have no value in each of these columns, separated by commas (one scan).
    Counts(&'a [String]),
    /// How many values of these columns more than one row holds, skipping rows with a NULL in them.
    Duplicates(&'a [String]),
    /// Up to five of those values, in order, each as its columns' text joined by `, `.
    DuplicateExamples(&'a [String]),
}
