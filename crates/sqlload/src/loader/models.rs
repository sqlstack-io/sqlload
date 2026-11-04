use std::collections::BTreeMap;
use std::time::Duration;

use crate::column_types::models::{ColumnType, Warehouse};
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::models::DuckDbDestination;
use crate::file_store::models::OpenFolder;
use crate::files_destination::models::{
    FilesDestination, PartitionFolder, PendingLoad, TableFiles, TablePath, TableState,
};
use crate::files_source::models::{FilesEncoding, FilesRead, FilesTable, Footer, TextChecks};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{
    Contract, FilesFormat, NewTables, OnSchemaChange, Policy, ProjectConfig, WriteStrategy,
};
use crate::python_sources::types::PythonHost;
use crate::routing::models::Route;
use crate::run_log::main::finished_record::finished_record;
use crate::run_log::main::later_cursor::later_cursor;
use crate::run_log::models::{RunOutcome, RunRecord};
use crate::schema_plan::models::{DesiredColumn, LiveColumn, SchemaPlan};
use crate::snowflake_destination::models::SnowflakeDestination;
use crate::store_paths::models::Location;
use crate::write_sql::models::{TableName, WriteMode};

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
}

impl RunEntry {
    /// The finished record for a run that ended as `outcome`.
    pub fn finished(&self, outcome: RunOutcome) -> RunRecord {
        let before = self.before.clone().filter(|_| !self.replaces);
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

/// A Python table read for its load: its data (`None` when there is nothing to write), its plan for that data, the schema changes made, and a remark.
#[derive(Debug)]
pub struct PythonRead {
    pub reader: Option<SourceReader>,
    pub table: TablePlan,
    pub changes: Vec<String>,
    pub note: Option<String>,
    /// The run record a logged table writes with its data.
    pub run: Option<RunEntry>,
}

/// A Python table's yielded data, held in temporary Arrow IPC files until its schema is known.
#[derive(Debug)]
pub struct SpilledBatches {
    /// Removed when dropped.
    pub directory: tempfile::TempDir,
    pub chunks: Vec<std::path::PathBuf>,
    /// The schema merged from every batch, with all-NULL columns as text.
    pub schema: arrow_schema::SchemaRef,
    /// The columns that only ever held NULL, which take the live or declared type.
    pub null_columns: std::collections::BTreeSet<String>,
    pub rows: u64,
}

/// An open destination.
#[derive(Debug, Clone)]
pub enum Destination {
    #[cfg(feature = "duckdb")]
    DuckDb(DuckDbDestination),
    Snowflake(Box<SnowflakeDestination>),
    /// Parquet files in an s3 or filesystem connection.
    Files(Box<FilesDestination>),
}

/// One load's staging table and the files written for it so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingSession {
    pub staging: TableName,
    pub load_id: String,
    /// The load time as an ISO 8601 UTC timestamp, written to every row.
    pub loaded_at: String,
    pub parts: usize,
}

/// A connected source, ready to read.
#[derive(Debug)]
pub enum SourceReader {
    Postgres(tokio_postgres::Client),
    Files(OpenFolder),
    /// A Python table's data, already read and spilled.
    Batches(SpilledBatches),
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
