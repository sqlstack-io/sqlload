use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use arrow_schema::SchemaRef;
use chrono::NaiveDate;
use parquet::arrow::ArrowWriter;

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::_helpers::layout::swap::{local_folder, staged_stem};
use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::project::models::{OutputEncoding, WriteStrategy};
use crate::schema_plan::models::DesiredColumn;
use crate::store_paths::main::escape_partition_value::escape_partition_value;
use crate::store_paths::main::unescape_partition_value::unescape_partition_value;
use crate::store_paths::models::{Location, StoreKind};

/// A target that writes tables as Parquet files into an s3 or filesystem connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesDestination {
    /// The connection's name, for messages and paths.
    pub connection: String,
    pub store: StoreKind,
    /// The most footers `plan` reads for one table.
    pub max_footer_reads: usize,
    /// The format and options of the files written.
    pub encoding: OutputEncoding,
}

/// How a partition folder's value is taken from the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionBy {
    /// `{cursor}`: the cursor value itself (int, date or text cursors).
    Value,
    /// `{cursor:date}`: the cursor's date (date or timestamp cursors).
    Date,
}

/// The partition folder of a table: `prefix`, the cursor's value, then `suffix`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFolder {
    pub prefix: String,
    pub suffix: String,
    pub by: PartitionBy,
}

impl PartitionFolder {
    /// The folder name for partition `value`, escaped as Hive escapes partition values.
    pub fn name(&self, value: &str) -> String {
        format!(
            "{}{}{}",
            self.prefix,
            escape_partition_value(value),
            self.suffix
        )
    }

    /// The partition value of folder `name` (escapes undone), if the name has this folder's shape.
    pub fn value_of(&self, name: &str) -> Option<String> {
        name.strip_prefix(self.prefix.as_str())?
            .strip_suffix(self.suffix.as_str())
            .filter(|value| !value.is_empty())
            .map(unescape_partition_value)
    }
}

impl std::fmt::Display for PartitionFolder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let placeholder = match self.by {
            PartitionBy::Value => "{cursor}",
            PartitionBy::Date => "{cursor:date}",
        };
        write!(formatter, "{}{placeholder}{}/", self.prefix, self.suffix)
    }
}

/// Where a table's files go: its folder, and the partition folder below it if the path has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePath {
    pub folder: Location,
    pub partition: Option<PartitionFolder>,
}

impl TablePath {
    /// The hidden folder beside the table folder that a `replace` or `--reload` of a local directory writes the table into, to swap it in at commit: `.<table folder>.sqlload-<load id>/`; `None` for other write modes, other stores and a table at the directory's root.
    pub fn staged_folder(&self, mode: &WriteMode, load_id: &str) -> Option<Location> {
        let rewrites = matches!(mode, WriteMode::Replace | WriteMode::Recreate);
        let (_, parent, name) = local_folder(&self.folder).filter(|_| rewrites)?;
        Some(Location {
            store: self.folder.store.clone(),
            prefix: format!("{parent}{}{load_id}/", staged_stem(name)),
        })
    }
}

/// What a table's path template is filled with.
#[derive(Debug, Clone, Copy)]
pub struct TablePathRequest<'a> {
    pub template: &'a str,
    pub connection: &'a str,
    pub store: &'a StoreKind,
    pub source: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
}

/// A partition value in comparable form.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKey {
    Int(i64),
    Date(NaiveDate),
    Text(String),
}

/// One partition's data files; `value` is `None` for a table without partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub value: Option<String>,
    pub files: Vec<StoredFile>,
}

/// A table's existing data files by partition, oldest partition first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableFiles {
    pub partitions: Vec<Partition>,
    /// Files that are not where the path puts them, so the table's layout cannot be trusted.
    pub problems: Vec<String>,
}

impl TableFiles {
    /// Every data file's key below the table folder.
    pub fn keys(&self) -> Vec<String> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.files.iter())
            .map(|file| file.relative_key.clone())
            .collect()
    }
}

/// What decides the cursor window of a `delete_insert` into partition folders.
#[derive(Debug, Clone, Copy)]
pub struct PartitionWindowRequest<'a> {
    pub by: PartitionBy,
    pub cursor_column: &'a str,
    pub cursor_type: &'a ColumnType,
    /// The newest existing partition's value; `None` when the table has none (or `--reload`).
    pub newest: Option<&'a str>,
    pub lookback: Option<Duration>,
    /// `--start-cursor-*`, already normalised for the cursor type.
    pub start: Option<&'a str>,
    /// `--end-cursor-*`, already normalised for the cursor type.
    pub end: Option<&'a str>,
    pub cursor_start: Option<&'a str>,
    pub reload: bool,
}

/// The cursor range read, widened to whole partitions, and a note for `plan`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindow {
    pub from: Option<String>,
    pub to: Option<String>,
    pub note: String,
}

/// What a load writes: the table, its columns, and the load's identity.
#[derive(Debug, Clone)]
pub struct WriterRequest<'a> {
    pub folder: OpenFolder,
    pub path: &'a TablePath,
    /// The cursor column partitions are taken from, when the path has a partition folder.
    pub cursor_column: Option<&'a str>,
    pub columns: &'a [DesiredColumn],
    pub load_id: &'a str,
    /// Names the files and the pending marker (see [`FilesWriter::file_id`]).
    pub file_id: &'a str,
    /// The load time as an ISO 8601 UTC timestamp, written to every row.
    pub loaded_at: &'a str,
    pub encoding: &'a OutputEncoding,
}

/// A file being filled in memory.
#[derive(Debug)]
pub struct OpenFile {
    pub body: FileBody,
    pub rows: usize,
}

/// A file's contents so far, in its format.
#[derive(Debug)]
pub enum FileBody {
    Parquet(Box<ArrowWriter<Vec<u8>>>),
    Csv(CsvOut),
}

/// CSV text written so far, compressed as it is written.
#[derive(Debug)]
pub enum CsvOut {
    Plain(Vec<u8>),
    Gzip(Box<flate2::write::GzEncoder<Vec<u8>>>),
    /// zstd frames compressed so far, and text waiting for the next frame.
    Zstd {
        done: Vec<u8>,
        pending: Vec<u8>,
    },
}

/// A load's writer: one open file per partition, flushed to the store as files fill up.
#[derive(Debug)]
pub struct FilesWriter {
    pub folder: OpenFolder,
    pub encoding: OutputEncoding,
    /// With `single_file`, each file's name before its suffix: the table folder's own name.
    pub single_name: String,
    pub partition: Option<PartitionFolder>,
    pub cursor_column: Option<String>,
    pub schema: SchemaRef,
    /// The load id the rows carry in `_sqlload_load_id`.
    pub load_id: String,
    /// Names this writer's files and its pending marker: one per commit of a load, so repairing an unfinished commit never touches an earlier one's files.
    pub file_id: String,
    pub loaded_at_micros: i64,
    /// Open files by partition value (empty for a table without partitions).
    pub open: BTreeMap<String, OpenFile>,
    /// Keys of the files written so far, below the table folder.
    pub written: Vec<String>,
    /// Partition values this load wrote rows to.
    pub partitions: BTreeSet<String>,
    pub rows: u64,
}

/// What decides whether a table's strategy and cursor suit its path.
#[derive(Debug, Clone, Copy)]
pub struct OutputCheck<'a> {
    pub strategy: WriteStrategy,
    pub cursor_column: Option<&'a str>,
    /// The cursor's type, when the cursor column is loaded.
    pub cursor_type: Option<&'a ColumnType>,
    pub partition: Option<&'a PartitionFolder>,
}

/// How far an unfinished load got, from its pending marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingPhase {
    /// Writing its new files: they are incomplete, so they are deleted.
    Writing,
    /// Deleting the files it replaces: its new files are complete, so the deletes are finished.
    Replacing,
}

/// A load that left its pending marker behind, and the files repairing it deletes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingLoad {
    pub load_id: String,
    pub phase: PendingPhase,
    /// Keys below the table folder that repair deletes: the load's own files while writing, the replaced ones while replacing.
    pub deletes: Vec<String>,
    /// The run record a logged table's load writes once its files are complete; repair writes it if the load stopped first.
    pub run: Option<PendingRun>,
}

/// What a pending marker records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    pub phase: PendingPhase,
    pub keys: Vec<String>,
    pub run: Option<PendingRun>,
}

/// A run record waiting in a `replacing` marker: the record object's folder (in the same store), its name and its JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRun {
    pub prefix: String,
    pub name: String,
    pub json: String,
}

/// A files table as listed: its trusted files by partition, loads to repair, and whether any data file exists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableState {
    /// Data files, without those an unfinished load leaves for repair to delete.
    pub listed: TableFiles,
    pub pending: Vec<PendingLoad>,
    pub exists: bool,
    /// Source files loads from a files source loaded, whose commits' files the table still holds.
    pub loaded: Vec<crate::files_source::models::LoadedFile>,
    /// Keys of load records none of whose commits' files the table holds any more.
    pub stale_loaded: Vec<String>,
}

/// The files a write replaces, worked out before anything is written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplacedFiles {
    /// Replaced whatever the load writes (everything, or partitions in the cursor window).
    pub always: Vec<String>,
    /// Each other partition's files, replaced if the load writes rows to it.
    pub by_partition: BTreeMap<String, Vec<String>>,
}

impl ReplacedFiles {
    /// Every replaced key, given the partitions the load wrote.
    pub fn keys(&self, written: &BTreeSet<String>) -> Vec<String> {
        let mut keys = self.always.clone();
        for value in written {
            keys.extend(self.by_partition.get(value).into_iter().flatten().cloned());
        }
        keys
    }
}

/// A table's folder, and what is needed to explain a clash with another table's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FolderClaim {
    /// Tells tables apart: the source and the source table.
    pub id: String,
    /// The table as `plan` names it.
    pub name: String,
    pub folder: Location,
    pub template: String,
    /// The source table's schema.
    pub schema: String,
    /// The folder a files source reads the table from; `None` for other sources.
    pub source: Option<Location>,
}

/// What decides the files a write replaces.
#[derive(Debug, Clone, Copy)]
pub struct ReplaceRequest<'a> {
    pub mode: &'a WriteMode,
    pub listed: &'a TableFiles,
    pub partition: Option<&'a PartitionFolder>,
    pub cursor_type: Option<&'a ColumnType>,
}

/// Parquet files' types: Arrow's, with no SQL of their own.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParquetFiles;
