use std::time::Duration;

use crate::column_types::models::ColumnType;
use crate::file_store::models::OpenFolder;
use crate::files_source::models::{BatchFooters, DataFile, FilesRead};
use crate::load_naming::models::Compat;
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
    /// Upsert: rows whose key matches a staged row are replaced; with `dedupe`, of the staged rows sharing a key only the one it ranks first is merged. Keys match NULL-safely (a NULL matches a NULL), except the `not_null_keys` columns, whose staged values are never NULL, which compare with `=`.
    Merge {
        unique_key: Vec<String>,
        dedupe: Option<MergeDedupe>,
        /// The key columns whose staged values are never NULL: the source declares them NOT NULL, or `null_keys = "fail"` refuses a commit holding a NULL in them.
        not_null_keys: Vec<String>,
    },
    /// Rows with `from <= cursor_column < to` (open-ended when `None`) are replaced by the staged rows; with `values`, the rows whose cursor is one of them (a files source's partitions) instead. With a `replace_key`, the rows holding a key value a staged row holds are replaced too (alone, without a cursor), matched as a merge matches keys: NULL-safely, except the `not_null_keys` columns, which compare with `=`.
    DeleteInsert {
        cursor_column: Option<String>,
        from: Option<String>,
        to: Option<String>,
        values: Vec<String>,
        replace_key: Vec<String>,
        /// The `replace_key` columns whose staged values are never NULL: the source declares them NOT NULL, or `null_keys = "fail"` refuses a commit holding a NULL in them.
        not_null_keys: Vec<String>,
    },
    /// The staged rows become the whole table, atomically.
    Replace,
    /// The table is rebuilt from the staged rows with the current columns and types (`--reload`), in one statement.
    Recreate,
    /// Captured changes (`kind = "postgres_cdc"`), applied only where their `_sqlload_lsn` is newer than what the table holds, so a replayed batch changes nothing.
    Changes(ChangeWrite),
    /// The staged rows are every key the source holds (`load --sync-deletes`): the table's rows loaded before `before` whose key none of them holds are marked deleted, matched as a merge matches keys.
    MarkMissing(MarkWrite),
}

/// How a key scan marks the table's rows whose key the source no longer holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkWrite {
    pub unique_key: Vec<String>,
    /// The key columns whose staged values are never NULL, which compare with `=`; the rest match NULL-safely.
    pub not_null_keys: Vec<String>,
    /// `_sqlload_deleted_at` is set to `before` on a row it is NULL on; otherwise the row is removed.
    pub soft: bool,
    /// When the key read started, RFC 3339 UTC: a row loaded since (its `_sqlload_loaded_at` not before it) may hold a key the read missed, so it stays.
    pub before: String,
    /// The most percent of the table's rows (those not marked yet) one scan may mark; more fails the table, marking none.
    pub max_drop: u8,
}

impl WriteMode {
    /// The key columns a commit by this mode matches its staged rows on, and the setting naming them: a merge's `unique_key`, a `delete_insert`'s `replace_key`; `None` for the other modes.
    pub fn staged_key(&self) -> Option<(&[String], &'static str)> {
        match self {
            WriteMode::Merge { unique_key, .. } => Some((unique_key, "unique_key")),
            WriteMode::DeleteInsert { replace_key, .. } if !replace_key.is_empty() => {
                Some((replace_key, "replace_key"))
            }
            _ => None,
        }
    }

    /// The key columns a merge, a change apply, a key scan or a `delete_insert` by `replace_key` matches rows on, each with whether its staged values may be NULL, so it matches NULL-safely; none for the other modes.
    pub fn matched_keys(&self) -> Vec<(&str, bool)> {
        let (unique_key, not_null_keys) = match self {
            WriteMode::Merge {
                unique_key,
                not_null_keys,
                ..
            } => (unique_key, not_null_keys),
            WriteMode::Changes(change) => (&change.unique_key, &change.not_null_keys),
            WriteMode::MarkMissing(mark) => (&mark.unique_key, &mark.not_null_keys),
            WriteMode::DeleteInsert {
                replace_key,
                not_null_keys,
                ..
            } => (replace_key, not_null_keys),
            _ => return Vec::new(),
        };
        unique_key
            .iter()
            .map(|key| (key.as_str(), !not_null_keys.contains(key)))
            .collect()
    }
}

/// Which of a commit's staged rows sharing a merge key is kept: the first by `sort` (a column, and whether its highest value wins; a NULL never wins), then the one with the largest `ordinal`, the last in source order. A NULL in the key is a value like any other, so rows sharing one are kept to one too, as the merge matches them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeDedupe {
    /// The staged column numbering rows in source order.
    pub ordinal: String,
    /// The `dedup_sort` column, and `true` when its highest value wins.
    pub sort: Option<(String, bool)>,
}

/// How a batch of captured changes is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeWrite {
    pub apply: ChangeApply,
    /// The source's key columns, which `merge` matches rows on.
    pub unique_key: Vec<String>,
    /// The key columns the source declares NOT NULL, which compare with `=`; the rest match NULL-safely.
    pub not_null_keys: Vec<String>,
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
    /// The further columns of a coalesced cursor: the maximum is then of the first non-NULL of `column` and these, as the source filter reads it.
    pub coalesce: &'a [String],
    pub column_type: &'a ColumnType,
    pub lookback: Option<Duration>,
    /// The table's `constant_columns` (name, type and value as text): only the rows holding these values count, so tables sharing a destination each read from their own maximum.
    pub scope: &'a [(String, ColumnType, String)],
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
    /// A wall-clock timestamp cursor's bound, and the fractional digits it is written with: its column's, as the source describes them, when it has no more, else `None` for every digit it has.
    Timestamp(chrono::NaiveDateTime, Option<u16>),
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
    /// How the table names the columns a load adds.
    pub compat: Compat,
    pub load_id: &'a str,
    /// The batch being filled, from 1.
    pub batch: u64,
    /// Batches go straight into the table, each committing.
    pub direct: bool,
    /// The batches land in a build table in their final types.
    pub builds: bool,
    /// The table's `constant_columns`: name, type and value as text, for a destination that loads the rows without SQLLoad seeing them.
    pub constants: &'a [(String, ColumnType, String)],
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
    /// How many values of these columns more than one row holds, a NULL equal to a NULL, as a merge matches keys.
    KeyDuplicates(&'a [String]),
    /// Up to five of those values, in order, each as its columns' text (`NULL` for a NULL) joined by `, `.
    KeyDuplicateExamples(&'a [String]),
    /// Up to five values of these columns, in order, of rows holding a NULL in any of them, each as its columns' text (`NULL` for a NULL) joined by `, `.
    NullKeyExamples(&'a [String]),
    /// Up to this many distinct values of these columns, in order, each as its columns' text (`NULL` for a NULL) joined by `, `.
    KeyValues(&'a [String], u64),
    /// The distinct values of `file_column` among the relation's rows whose `key` values (as `KeyValues` gives them) a row of `staged` holds, other than `file`, in order: the files a commit's key values already come from.
    KeySources {
        key: &'a [String],
        staged: &'a TableName,
        file_column: &'a str,
        file: &'a str,
    },
}
