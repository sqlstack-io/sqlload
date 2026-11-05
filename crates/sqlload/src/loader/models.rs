use std::collections::BTreeMap;
use std::time::Duration;

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::dispatch::models::{Destination, Writer};
use crate::adapters::files::models::{
    FilesWriter, PartitionFolder, PendingLoad, ReplacedFiles, TableFiles, TablePath, TableState,
};
use crate::adapters::snowflake::models::PlannedStage;
use crate::adapters::snowflake::models::StageRoute;
use crate::batch_plan::models::{FileGroup, WindowPlan};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::file_store::models::OpenFolder;
use crate::files_source::models::{FilesEncoding, FilesRead, FilesTable, Footer, TextChecks};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{
    BatchStep, Contract, FilesFormat, NewTables, OnSchemaChange, Policy, ProjectConfig,
    WriteStrategy,
};
use crate::python_sources::types::PythonHost;
use crate::routing::models::Route;
use crate::run_log::main::finished_record::finished_record;
use crate::run_log::main::later_cursor::later_cursor;
use crate::run_log::main::next_stamp::next_stamp;
use crate::run_log::models::{RunOutcome, RunRecord, UnreadableRecord};
use crate::schema_plan::models::{DesiredColumn, LiveColumn, SchemaPlan};
use crate::store_paths::models::Location;

/// Where a setting's value came from, for `plan`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingOrigin {
    Table,
    Source,
    Default,
}

impl std::fmt::Display for SettingOrigin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Table => "table",
            Self::Source => "source",
            Self::Default => "default",
        })
    }
}

/// An incremental cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorSpec {
    pub column: String,
    pub start: Option<String>,
    pub lookback: Option<Duration>,
    /// `>=` the destination's maximum (true) or `>` it.
    pub inclusive: bool,
}

/// One table to load, with every setting resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    pub source_name: String,
    pub source_table: SourceTable,
    /// The target the table loads into.
    pub target: String,
    pub destination: TableName,
    pub warehouse: Warehouse,
    pub strategy: WriteStrategy,
    pub strategy_origin: SettingOrigin,
    pub unique_key: Vec<String>,
    pub cursor: Option<CursorSpec>,
    /// Read and commit the cursor range one window of this size at a time.
    pub batch: Option<BatchStep>,
    /// Tests only (`test_loader_route`, absent from released builds): SQLLoad reads a files source's files itself, the reference side of the live parity test.
    pub test_loader_route: bool,
    /// Where an integer `batch`'s windows end without `--end-cursor-int`: the Postgres source's cursor maximum, read at plan.
    pub source_end: Option<SourceEnd>,
    pub pinned: BTreeMap<String, ColumnType>,
    /// Source columns not loaded, with the rule that dropped each.
    pub excluded_columns: Vec<(String, String)>,
    /// Columns loaded masked (read as text), and the hmac key.
    pub masked: MaskRules,
    pub contract: Contract,
    pub on_schema_change: OnSchemaChange,
    pub type_change_rebuild: Policy,
    /// Whether a Python table's `replace` may empty the table when its function yields no rows.
    pub allow_empty: bool,
    /// Whether the table's position is kept in the run log (`state = "logged"`).
    pub logged: bool,
    /// Listed under `[sources.*.tables]`, so never treated as a new table.
    pub listed: bool,
    pub new_tables: NewTables,
    /// The files behind a files source; `None` for database tables.
    pub files: Option<FilesSource>,
    /// The table's folder template in a files target; `None` for warehouse targets.
    pub destination_path: Option<String>,
    /// The table function behind a Python table; `None` for declarative sources.
    pub python: Option<PythonTable>,
}

/// A table declared in Python: its columns are inferred from what the function yields at load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonTable {
    /// The file declaring it, relative to the project directory.
    pub module: String,
    /// Columns declared with `columns` or `mask`; the rest are inferred at load.
    pub declared: Vec<String>,
}

/// The cursor range a load reads: `from <= cursor < to` (`from < cursor` when not inclusive), each open when `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadWindow {
    pub column: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub inclusive: bool,
    /// The data's cursor maximum without lookback, when the plan read it.
    pub maximum: Option<String>,
}

/// Where a files source's files are and what they hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesSource {
    pub location: Location,
    pub table: FilesTable,
    /// Declared partition columns and their types, by name; a file column of the same name is replaced by the path's value.
    pub partition_types: Vec<(String, ColumnType)>,
    pub encoding: FilesEncoding,
    /// Column names and patterns JSON and CSV reads check keys and headers against.
    pub checks: TextChecks,
    /// Notes `plan` shows on the table, such as pinned columns no sampled file has.
    pub notes: Vec<String>,
    /// `copy_to_internal_stage`: files no external stage reads are uploaded unchanged to an internal stage, and loaded from there by the warehouse.
    pub copy_to_internal_stage: bool,
}

/// A table that matched a source's selection but is not loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedTable {
    pub source_name: String,
    pub source_table: String,
    pub reason: String,
}

/// The settings for tables of one project, before any destination is read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedTables {
    pub specs: Vec<TableSpec>,
    pub skipped: Vec<SkippedTable>,
    /// Problems with one table's settings, as `source_table: problem`.
    pub problems: Vec<String>,
}

/// One table's plan against the live destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePlan {
    pub spec: TableSpec,
    pub read_columns: Vec<ReadColumn>,
    pub live: Option<Vec<LiveColumn>>,
    pub schema: SchemaPlan,
    pub mode: WriteMode,
    /// The source filter for incremental reads, already rendered for Postgres.
    pub source_filter: Option<String>,
    /// How the cursor window was chosen, for `plan`.
    pub cursor_note: Option<String>,
    /// The cursor range read, for sources that filter it themselves (Python tables).
    pub window: Option<ReadWindow>,
    /// How the data moves from the source to the destination.
    pub route: Option<Route>,
    /// The files to read, for a files source.
    pub files: Option<FilesRead>,
    /// Where and how the files are written, for a files target.
    pub output: Option<FilesOutput>,
    /// The destination table exists (a warehouse table, or data files in a files table's folder).
    pub exists: bool,
    /// The run log of a `state = "logged"` table, read at plan.
    pub logged: Option<LoggedPlan>,
    /// How the rows are cut into commits.
    pub commits: CommitRule,
    /// How the rows reach Snowflake.
    pub stage: Option<StageRoute>,
    /// The stage `apply` must create.
    pub planned_stage: Option<PlannedStage>,
}

/// A logged table's run log and what it means for this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedPlan {
    /// Every record, oldest first.
    pub records: Vec<RunRecord>,
    /// The record holding the current state.
    pub current: Option<RunRecord>,
    /// The data's cursor maximum, when it disagrees with the logged cursor: the data wins and the load appends a reconciliation.
    pub disagreement: Option<String>,
    /// Records that cannot be used, by `sqlload state show`.
    pub unreadable: Vec<UnreadableRecord>,
    /// The warehouse's `_sqlload_runs` existed at plan, so loading creates nothing.
    pub runs_table: bool,
}

impl RunEntry {
    /// The finished record for a run that ended as `outcome`.
    pub fn finished(&self, outcome: RunOutcome) -> RunRecord {
        let before = self.before.clone().filter(|_| !self.replaces);
        let outcome = RunOutcome {
            recorded_at: next_stamp(&outcome.recorded_at, self.newest.as_deref()),
            ..outcome
        };
        finished_record(&self.record, self.cursor_type.as_ref(), before, outcome)
    }

    /// Fold one written batch's cursor maximum into `maximum`.
    pub fn later(&self, maximum: Option<String>, batch_maximum: Option<String>) -> Option<String> {
        match &self.cursor_type {
            Some(column_type) => later_cursor(maximum, batch_maximum, column_type),
            None => None,
        }
    }
}

/// A run record to write with a logged table's data, completed once the rows are written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEntry {
    /// The record without its row count, files and cursor after.
    pub record: RunRecord,
    pub cursor_type: Option<ColumnType>,
    /// The data's cursor maximum before the load, which the load keeps unless it replaces the data.
    pub before: Option<String>,
    /// The load replaces every row, so the data's maximum is the loaded rows' maximum.
    pub replaces: bool,
    /// The log's newest record time, which this record must follow.
    pub newest: Option<String>,
}

/// A files target's side of one table's plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesOutput {
    pub path: TablePath,
    pub format: FilesFormat,
    /// Data files under the table's folder now.
    pub listed: usize,
    /// Footers read for the live schema (the newest partition's files).
    pub footers_read: usize,
    pub max_footer_reads: usize,
    /// How the strategy writes and replaces files, for `plan`.
    pub notes: Vec<String>,
    /// Loads that did not finish; `load` repairs them first.
    pub pending: Vec<PendingLoad>,
}

/// The plan for a whole project.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectPlan {
    pub tables: Vec<TablePlan>,
    pub skipped: Vec<SkippedTable>,
    /// Problems with the project's settings for particular tables.
    pub problems: Vec<String>,
    /// Things to know that block nothing, such as a selector matching no table.
    pub warnings: Vec<String>,
}

/// Choices for one `plan`, `apply` or `load`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanOptions {
    /// Source names, destination table names or `schema.table` source names; empty selects everything.
    pub select: Vec<String>,
    /// `--allow-type-change-rebuild`.
    pub rebuild_allowed: bool,
    /// `--reload`: rebuild each selected table from the whole source, ignoring the destination's cursor.
    pub reload: bool,
    /// `--start-cursor-*`: read from this cursor value instead of the destination's maximum.
    pub start_cursor: Option<String>,
    /// `--end-cursor-*`: read only cursor values below this one.
    pub end_cursor: Option<String>,
    /// `--threads`: the most tables loading at once, overriding the project's `threads`.
    pub threads: Option<usize>,
    /// The bytes a batch holds before it may commit, instead of about 1 GB (a hidden flag, for tests).
    pub batch_bytes: Option<usize>,
}

/// What one Python table's load needs.
#[derive(Debug)]
pub struct PythonLoad<'a> {
    pub project: &'a ProjectConfig,
    pub table: &'a TablePlan,
    pub destination: &'a Destination,
    pub host: std::sync::Arc<dyn PythonHost>,
    pub options: &'a PlanOptions,
}

/// What a whole load works with.
#[derive(Debug)]
pub struct LoadRun<'a> {
    pub project: &'a ProjectConfig,
    pub destinations: &'a BTreeMap<String, Destination>,
    /// Runs Python tables; `None` in the standalone executable.
    pub host: Option<std::sync::Arc<dyn PythonHost>>,
    pub options: &'a PlanOptions,
}

/// What every table of a load shares, owned so each table can run on a thread of its own.
#[derive(Debug)]
pub struct SharedRun {
    pub project: ProjectConfig,
    pub destinations: BTreeMap<String, Destination>,
    pub host: Option<std::sync::Arc<dyn PythonHost>>,
    pub options: PlanOptions,
}

/// One table's load: its rows, or why it failed (other tables still load).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOutcome {
    pub table: String,
    pub loaded: Result<TableLoaded, String>,
}

/// What a table's load did: rows written, schema changes made on the way (Python tables), and a remark for the `loaded` line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableLoaded {
    pub rows: u64,
    /// `created ...` and `altered ...` lines.
    pub changes: Vec<String>,
    pub note: Option<String>,
}

/// A table's columns planned once more during its load, for rows showing new columns.
#[derive(Debug)]
pub struct ReplannedColumns {
    /// The table's spec, with a Python table's live columns among its source's.
    pub spec: TableSpec,
    pub read_columns: Vec<ReadColumn>,
    pub live: Option<Vec<LiveColumn>>,
    pub schema: SchemaPlan,
}

/// A Python table's load as its function's batches arrive: the plan for the columns seen so far, and where the batches go.
#[derive(Debug)]
pub struct PythonFeed {
    pub table: TablePlan,
    /// The schema merged from every batch so far, all-NULL columns as `Null`.
    pub merged: arrow_schema::Schema,
    pub rows: u64,
    /// Rows the function yielded outside the cursor window, which were skipped.
    pub outside: u64,
    /// Opened once the table exists for the data.
    pub publisher: Option<Publisher>,
    /// `created ...` and `altered ...` lines.
    pub changes: Vec<String>,
    /// `ctx.state` as the run started, or as the function left it at the end of the latest window.
    pub state: Option<String>,
    /// The time of the reconciliation this load appended, which its records follow.
    pub reconciled: Option<String>,
}

/// A Postgres table's integer cursor as plan found it, ending its `batch` windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceEnd {
    /// The table has no cursor value, so there is nothing to read.
    Empty,
    /// The largest value: windows end one past it.
    Maximum { maximum: String, end: String },
}

/// How a table's rows are cut into commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRule {
    /// Each window of the table's `batch`, read and committed by itself.
    Windows(WindowPlan),
    /// Read in cursor order (an index leads with the cursor, NULLs first), committing about every 1 GB, only between distinct cursor values; `delete_insert` replaces `[previous cut, next cut)` each time.
    Ordered {
        column: String,
        cursor_type: ColumnType,
    },
    /// About every 1 GB (no cursor, so no position to keep in order).
    Size,
    /// A files source's batches of whole partitions.
    FileGroups(Vec<FileGroup>),
    /// The whole read is one commit.
    Whole(WholeReason),
}

/// Why a table's read commits once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WholeReason {
    /// An unordered cursor read: committing part of it could skip rows.
    Unordered,
    /// A text cursor, whose order the source and the destination may collate differently.
    TextCursor,
    /// `append` without a cursor: a rerun after a partial commit would duplicate what committed.
    AppendWithoutCursor,
    /// `batch = "whole"`.
    Chosen,
    /// The table cannot load, so it has no commits.
    Blocked,
}

/// What the rows of one commit replace under `delete_insert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRange {
    /// The planned write: the whole read's range.
    Planned,
    /// A cursor window, `from <= cursor < to`.
    Window {
        from: Option<String>,
        to: Option<String>,
    },
    /// A files source's partitions, by the cursor's value.
    Values(Vec<String>),
}

/// One table's load as it commits batch by batch: where the batches go, and what is staged but not yet committed.
#[derive(Debug)]
pub struct Publisher {
    pub table: TablePlan,
    /// The table's own connection to the destination.
    pub destination: Destination,
    /// A logged table's record, updated at every commit.
    pub run: Option<RunEntry>,
    pub load_id: String,
    /// The load time as an ISO 8601 UTC timestamp, written to every row.
    pub loaded_at: String,
    /// Batches go straight into the table, each committing with its `COPY` (`append`); otherwise into `staging`.
    pub direct: bool,
    /// Only the end of the run commits (`replace` and `--reload`, which swap or rebuild the whole table).
    pub at_end: bool,
    /// `replace` or `--reload` building the table it swaps in: batches land in `staging` in their final types.
    pub builds: bool,
    /// The indexes to re-create on the build table once it is swapped in.
    pub indexes: Vec<String>,
    pub staging: Option<TableName>,
    /// The batch being filled, from 1.
    pub batch: u64,
    /// How the batches get into the destination.
    pub writer: Writer,
    /// Arrow bytes of the batch so far, which measure it when the writer stages no files.
    pub arrow_bytes: usize,
    pub batch_rows: u64,
    /// Rows in `staging` not committed yet, and what they replace.
    pub waiting_rows: u64,
    pub waiting_range: Option<CommitRange>,
    pub committed_rows: u64,
    /// Commits made, from 0.
    pub commits: u64,
    /// The largest cursor written so far (logged tables).
    pub maximum: Option<String>,
    /// `ctx.state` when the newest staged batch crossed, and when the last commit was made.
    pub state: Option<String>,
    pub committed_state: Option<String>,
    /// The bytes a batch holds before it may commit.
    pub batch_bytes: usize,
    /// A files target's folder and the commit being written.
    pub files: Option<FilesSink>,
}

/// A files target table's load: its folder, what the load replaces, and the files of the commit being written.
#[derive(Debug)]
pub struct FilesSink {
    pub output: FilesOutput,
    pub folder: OpenFolder,
    pub replaced: ReplacedFiles,
    pub writer: Option<FilesWriter>,
    /// Replaced files already deleted by earlier commits.
    pub deleted: std::collections::BTreeSet<String>,
}

/// A connected source, ready to read.
#[derive(Debug)]
pub enum SourceReader {
    /// The client, and the connections that help it read a large table in parallel.
    Postgres(tokio_postgres::Client, Vec<tokio_postgres::Client>),
    Files(OpenFolder),
}

/// What decides one files table's cursor window.
#[derive(Debug, Clone, Copy)]
pub struct FilesWindowRequest<'a> {
    pub spec: &'a TableSpec,
    pub options: &'a PlanOptions,
    pub cursor_type: Option<&'a ColumnType>,
    pub partition: Option<&'a PartitionFolder>,
    /// The table's files now, newest partition last.
    pub listed: &'a TableFiles,
    /// The newest partition's footers, whose statistics hold an append cursor's maximum.
    pub footers: &'a [Footer],
    /// The source's read columns, holding the cursor's select expression.
    pub read_columns: &'a [ReadColumn],
}

/// A files table's newest load: its footers and schema.
#[derive(Debug, Clone)]
pub struct LiveFiles {
    pub footers: Vec<Footer>,
    /// The merged schema of the newest load's files in the newest partition; `None` before any file exists.
    pub live: Option<Vec<LiveColumn>>,
}

/// A files table ready to plan: its columns, path, cursor type, open folder and current state.
#[derive(Debug, Clone)]
pub struct FilesContext {
    pub read_columns: Vec<ReadColumn>,
    pub desired: Vec<DesiredColumn>,
    pub path: TablePath,
    pub cursor_type: Option<ColumnType>,
    pub folder: OpenFolder,
    pub state: TableState,
}

/// What `sqlload state` does to the selected logged tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateKind {
    Show,
    Verify,
    Reconcile,
    Reset,
}

/// A `sqlload state` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequest {
    pub kind: StateKind,
    /// Append the events (`--yes`) instead of only showing them.
    pub append: bool,
    /// The reset position (`--to`); `None` clears it.
    pub to: Option<String>,
}
