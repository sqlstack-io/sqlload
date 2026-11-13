use crate::adapters::contract::models::BoundParameter;
use crate::adapters::contract::models::ChangeApply;
use crate::load_naming::models::Naming;
use crate::row_batches::models::Unnest;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::models::{Destination, Writer};
use crate::adapters::files::models::{
    FilesWriter, PartitionFolder, PendingLoad, ReplacedFiles, TableFiles, TablePath, TableState,
};
use crate::adapters::snowflake::models::PlannedStage;
use crate::adapters::snowflake::models::StageRoute;
use crate::batch_plan::models::{FileGroup, WindowPlan};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::models::{
    AfterLoad, AfterLoadReport, FilesEncoding, FilesRead, FilesTable, Footer, LoadedFile,
    TextChecks,
};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{
    BatchStep, Contract, NewTables, OnDuplicateKeys, OnSchemaChange, OutputEncoding, Policy,
    ProjectConfig, SortDirection, TableChecks, WriteStrategy,
};
use crate::python_sources::types::PythonHost;
use crate::routing::models::{Route, Store};
use crate::run_log::main::finished_record::finished_record;
use crate::run_log::main::later_cursor::later_cursor;
use crate::run_log::main::next_stamp::next_stamp;
use crate::run_log::models::{RunOutcome, RunRecord, UnreadableRecord};
use crate::schema_plan::models::{DesiredColumn, LiveColumn, SchemaPlan};
use crate::snowflake_source::models::SnowflakeReader;
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
    /// Read only cursor values before now plus this (`lookahead`).
    pub lookahead: Option<Duration>,
    /// `lookback_from = "now"`: incremental runs read from now minus `lookback`, not from the destination's maximum minus it.
    pub from_now: bool,
    /// `append_cursor_inclusive`: `append` reads from the destination's maximum itself (true) rather than after it; a first run reads from `cursor_start` inclusively either way.
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
    /// The table's required description, given to the destination table as its comment.
    pub description: String,
    pub warehouse: Warehouse,
    pub strategy: WriteStrategy,
    pub strategy_origin: SettingOrigin,
    pub unique_key: Vec<String>,
    /// What a merge does with rows of one commit sharing `unique_key`.
    pub on_duplicate_keys: OnDuplicateKeys,
    /// The column and direction that pick which of the rows sharing a key a merge keeps; without it, the last in source order.
    pub dedup_sort: Option<(String, SortDirection)>,
    pub cursor: Option<CursorSpec>,
    /// Read and commit the cursor range one window of this size at a time.
    pub batch: Option<BatchStep>,
    /// `constant_columns`: each column's name, type and value as text, the same in every row; added after the source's columns (a files source's reader fills them itself).
    pub constants: Vec<(String, ColumnType, String)>,
    /// Whether another configured table loads into the same destination table, so this one's incremental cursor reads only the rows of its own `constants`.
    pub shared_destination: bool,
    /// Tests only (`test_loader_route`, absent from released builds): SQLLoad reads a files source's files itself, the reference side of the live parity test.
    pub test_loader_route: bool,
    /// Where an integer `batch_size`'s windows end without `--end-cursor`: the Postgres source's cursor maximum, read at plan.
    pub source_end: Option<SourceEnd>,
    pub pinned: BTreeMap<String, ColumnType>,
    /// The child tables' declared columns under `unnest`, each by its dotted path (`items.sku`), which an enforced contract loads alone, as it does the table's `pinned`.
    pub child_pinned: BTreeMap<String, ColumnType>,
    /// Source columns not loaded, with the rule that dropped each.
    pub excluded_columns: Vec<(String, String)>,
    /// Columns loaded masked (read as text), and the hmac key.
    pub masked: MaskRules,
    pub contract: Contract,
    pub on_schema_change: OnSchemaChange,
    /// `auto_widen = true`: an inferred column whose values outgrow it widens along the ladder instead of failing.
    pub auto_widen: bool,
    pub type_change_rebuild: Policy,
    /// Whether a Python table's `replace` may empty the table when its function yields no rows.
    pub allow_empty: bool,
    /// Whether the table's position is kept in the run log (`state = "logged"`).
    pub logged: bool,
    /// What a files source does with each file once its rows commit.
    pub after_load: AfterLoad,
    /// Approved for loading as soon as it appears: named exactly in `include`, or declared (files and Python tables); a `[sources.*.tables]` entry alone never approves.
    pub approved: bool,
    pub new_tables: NewTables,
    /// The files behind a files source; `None` for database tables.
    pub files: Option<FilesSource>,
    /// The table's folder template in a files target; `None` for warehouse targets.
    pub destination_path: Option<String>,
    /// The top-level objects expanded one level into `<object>__<key>` columns (Python tables only).
    pub flatten: Vec<String>,
    /// Every row gets a unique id column.
    pub row_id: bool,
    /// A child table of a merge: always staged, never written straight to the table, its staged rows carrying their root row's ordinal (`_sqlload_root_row`, staging only), so the rows of roots the merge drops leave before the commit.
    pub root_rows: bool,
    /// The list fields whose items become rows of child tables.
    pub unnest: Unnest,
    /// How many levels of child tables `unnest` makes at most; deeper lists stay JSON.
    pub max_depth: u32,
    /// The table function behind a Python table; `None` for declarative sources.
    pub python: Option<PythonTable>,
    /// What a Google Sheets table reads; `None` for other sources.
    pub sheet: Option<crate::sheets_source::models::SheetRead>,
    /// How a `postgres_cdc` table's captured changes are written; `None` for other sources.
    pub cdc: Option<CdcSpec>,
    /// The bastion a Postgres source is reached through, as `ssh user@host:port`.
    pub tunnel: Option<String>,
    /// The checks its staged rows must pass before they are published (the table's own, else its source's).
    pub checks: Option<TableChecks>,
    /// How a warehouse source's table is read; `None` for other sources.
    pub warehouse_source: Option<WarehouseSource>,
    /// How the table is laid out when it is created, where the warehouse takes a layout (ClickHouse).
    pub layout: TableLayout,
    /// How the load names the columns it adds, and under dlt's naming its tables and columns.
    pub naming: Naming,
}

/// A warehouse source's table: the query behind it, if any, and the route its warehouse reads it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarehouseSource {
    /// The source's kind as written, such as `snowflake`.
    pub system: &'static str,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<String>,
    pub route: WarehouseRoute,
}

/// How a warehouse source's rows leave the warehouse; each warehouse chooses its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarehouseRoute {
    /// The warehouse writes Parquet into `store`, which SQLLoad reads back and removes; `stage` says why there, for `plan`.
    Unload { store: Store, stage: String },
    /// SQLLoad runs the query and reads its rows as Arrow.
    Query,
}

/// A created table's layout, with SQLBuild's ClickHouse field names; every field unset by default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableLayout {
    /// The table engine with its arguments.
    pub engine: Option<String>,
    /// Sorting key expressions.
    pub order_by: Option<Vec<String>>,
    /// The partitioning expression.
    pub partition_by: Option<String>,
    /// Primary key expressions, a prefix of the sorting key.
    pub primary_key: Option<Vec<String>>,
    /// Table settings by name.
    pub table_settings: std::collections::BTreeMap<String, crate::project::models::LayoutSetting>,
}

impl TableLayout {
    /// Whether no layout field is set.
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// A `postgres_cdc` table's stream: where it comes from and how its changes are applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcSpec {
    pub apply: ChangeApply,
    pub slot: String,
    pub publication: String,
    /// How often `load --follow` writes the changes it has read.
    pub batch_interval: std::time::Duration,
}

/// A table declared in Python: its columns are inferred from what the function yields at load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonTable {
    /// The file declaring it, relative to the project directory.
    pub module: String,
    /// Columns declared with `columns` or `mask`; the rest are inferred at load.
    pub declared: Vec<String>,
    /// Under `unnest = "all"`, the columns chosen from the yielded values as list fields or not so far.
    pub lists: crate::row_batches::types::ListChoices,
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
    /// The partition columns that are `constant_columns`, the same in every file.
    pub constants: Vec<String>,
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

/// A source's tables, resolved before any table is planned.
#[derive(Debug)]
pub struct FoundSource<'a> {
    pub name: &'a String,
    pub source: &'a crate::project::models::SourceConfig,
    pub resolved: ResolvedTables,
    pub files: Option<FilesSource>,
}

/// Every source's resolved tables, the folders they claim, all of them as configured, and the plan holding the problems of sources that could not be read.
#[derive(Debug, Default)]
pub struct FoundSources<'a> {
    pub sources: Vec<FoundSource<'a>>,
    pub claims: Vec<crate::adapters::files::models::FolderClaim>,
    pub configured: Vec<TableSpec>,
    pub plan: ProjectPlan,
    /// The sources whose tables could not be listed, selected or not, so the tables they would load are unknown.
    pub unresolved: Vec<String>,
    /// The destination table and source of each files source skipped as drained (see [`DrainedFiles`]), names a selector may use.
    pub drained: Vec<(String, String)>,
}

/// The settings for tables of one project, before any destination is read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedTables {
    pub specs: Vec<TableSpec>,
    pub skipped: Vec<SkippedTable>,
    /// Problems with one table's settings, as `source_table: problem`.
    pub problems: Vec<String>,
    /// Warnings about reaching the source, such as reading it in plain text.
    pub warnings: Vec<String>,
    /// A files source whose folder `after_load` emptied, when the selection takes it.
    pub drained: Option<DrainedFiles>,
}

/// A files source with an existing, readable folder that `after_load` emptied: skipped with nothing to load when its destination `table` exists in `target`, a problem when it does not, as a path that never held the table's files looks the same.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainedFiles {
    /// Whether the selection takes it; one it does not still counts as unknown to tables sharing its destination.
    pub in_scope: bool,
    pub target: String,
    pub table: TableName,
    pub skipped: SkippedTable,
    pub problem: String,
    /// Its settings resolved as a table with no columns, so tables sharing its destination are checked against them as against any configured table; `None` when they do not resolve.
    pub spec: Option<TableSpec>,
}

/// The selected tables of one project for `contract`, before any destination is read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectedTables {
    pub tables: Vec<SelectedTable>,
    /// Problems with sources, tables' settings or selectors.
    pub problems: Vec<String>,
}

/// One selected table and what its source would load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedTable {
    pub spec: TableSpec,
    /// The columns `plan` would ask the table to have, from its source's catalog or file footers as listed, pinned ones included; or why they cannot be read.
    pub source_columns: Result<Vec<DesiredColumn>, Vec<String>>,
}

/// One table's plan against the live destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePlan {
    pub spec: TableSpec,
    pub read_columns: Vec<ReadColumn>,
    pub live: Option<Vec<LiveColumn>>,
    pub schema: SchemaPlan,
    pub mode: WriteMode,
    /// The source filter for incremental reads, already rendered in the source's dialect.
    pub source_filter: Option<String>,
    /// The values bound to `source_filter`'s `?` placeholders, in order: an ODBC source's cursor bounds.
    pub source_parameters: Vec<BoundParameter>,
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
    /// Records that cannot be used, by `sqlload logged-state show`.
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
    pub encoding: OutputEncoding,
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
    /// The `problems` a rerun may get past: a source that could not be reached.
    pub retryable_problems: Vec<String>,
}

impl ProjectPlan {
    /// Whether everything that stops the plan is something a rerun may get past: no table is blocked, and every problem is a source that could not be reached.
    pub fn only_retryable_problems(&self) -> bool {
        self.tables
            .iter()
            .all(|table| table.schema.blocked.is_none())
            && self
                .problems
                .iter()
                .all(|problem| self.retryable_problems.contains(problem))
    }

    /// The plan without its `postgres_cdc` tables, which load together per source rather than one by one.
    pub fn uncaptured(&self) -> Self {
        Self {
            tables: self
                .tables
                .iter()
                .filter(|table| table.spec.cdc.is_none())
                .cloned()
                .collect(),
            ..self.clone()
        }
    }
}

impl TablePlan {
    /// Where the table comes from and goes, and how it is written, for its outcome.
    pub fn origin(&self) -> OutcomeOrigin {
        OutcomeOrigin {
            source: self.spec.source_name.clone(),
            target: self.spec.target.clone(),
            strategy: self.spec.strategy,
        }
    }

    /// The name the table's load is reported under: its folder in a files target, which has no schema, and its display name elsewhere.
    pub fn outcome_name(&self) -> String {
        let adapter = sql_adapter(self.spec.warehouse);
        match &self.output {
            Some(output) if !adapter.runs_sql() => output.path.folder.to_string(),
            _ => adapter.render_display_name(&self.spec.destination),
        }
    }
}

/// Choices for one `plan`, `apply` or `load`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanOptions {
    /// Source names, destination table names or `schema.table` source names; empty selects everything.
    pub select: Vec<String>,
    /// Tables `--exclude` leaves out, named the same ways; it wins over `select`.
    pub exclude: Vec<String>,
    /// `--allow-type-change-rebuild`.
    pub rebuild_allowed: bool,
    /// `--reload`: rebuild each selected table from the whole source, ignoring the destination's cursor.
    pub reload: bool,
    /// `--start-cursor` (or `-ts`/`-int`): read from this cursor value instead of the destination's maximum.
    pub start_cursor: Option<String>,
    /// `--end-cursor` (or `-ts`/`-int`): read only cursor values below this one.
    pub end_cursor: Option<String>,
    /// `--concurrency`: the most tables loading at once, overriding the project's `concurrency`.
    pub concurrency: Option<usize>,
    /// The bytes a batch holds before it may commit, instead of about 1 GB (a hidden flag, for tests).
    pub batch_bytes: Option<usize>,
    /// `--resnapshot`: drop and recreate each selected `postgres_cdc` source's slot and copy every table again.
    pub resnapshot: bool,
    /// `--fail-fast`: once a table fails, start no other; tables already loading finish.
    pub fail_fast: bool,
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
    /// The table's source, target and write strategy; `None` for a failure of no one table (a `load --follow` with none).
    pub origin: Option<OutcomeOrigin>,
    pub loaded: Result<TableLoaded, String>,
    /// Whether a failed load may succeed when run again unchanged; `false` once loaded.
    pub retryable: bool,
    pub elapsed: std::time::Duration,
    /// Never started, because `--fail-fast` stopped the load after another table failed; `loaded` holds why.
    pub skipped: bool,
}

impl TableOutcome {
    /// Whether this table needs nothing a rerun cannot give it: it loaded, or its failure is retryable.
    pub fn passes_on_rerun(&self) -> bool {
        match &self.loaded {
            Ok(_) => true,
            Err(_) => self.retryable,
        }
    }
}

/// Where a loaded table comes from and goes, and how it is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeOrigin {
    pub source: String,
    pub target: String,
    pub strategy: WriteStrategy,
}

/// What a table's load did: rows written, schema changes made on the way (Python tables), and a remark for the `loaded` line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableLoaded {
    pub rows: u64,
    /// `created ...` and `altered ...` lines.
    pub changes: Vec<String>,
    pub note: Option<String>,
    /// What a files source's `after_load` did with the files it loaded; `None` when it keeps them.
    pub after_load: Option<AfterLoadReport>,
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
    /// Each column seen so far, and whether flattening a listed object made it, so no column comes from both.
    pub flattened: std::collections::BTreeMap<String, bool>,
}

/// A Postgres table's integer cursor as plan found it, ending its `batch_size` windows.
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
    /// Each window of the table's `batch_size`, read and committed by itself.
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
    /// `batch_size = "none"`.
    Chosen,
    /// `single_file`: each file is written once, whole.
    SingleFile,
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
    /// Rows staged so far, the next row's `_sqlload_row` when a merge dedupes by source order.
    pub staged_ordinal: i64,
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
    /// A files source's merge: the file (`name|version`) the next commit merges, recorded in the run log with it.
    pub merged_file: Option<String>,
    /// The files fed since the last commit, as they were read, settled by the next one (`after_load`).
    pub fed_files: Vec<StoredFile>,
    /// The files whose rows are committed, as they were read, which `after_load` deletes or moves.
    pub settled_files: Vec<StoredFile>,
    /// The child tables `unnest` fills from the table's rows; `None` without `unnest`.
    pub children: Option<ChildTables>,
}

/// The child tables of one table's load, each planned from its rows as they arrive.
#[derive(Debug)]
pub struct ChildTables {
    /// Under `unnest = "all"`, the fields at every level chosen as list fields or not so far: the plan's, then as the rows decide.
    pub choices: crate::row_batches::types::ListChoices,
    /// Under `unnest = "all"`, the child tables there before the load, as the destination stores their names: an earlier load chose the fields they hold as list fields, at every level.
    pub live: Vec<String>,
    /// A connection of their own that plans, creates and alters them before each has its publisher.
    pub destination: Destination,
    pub options: PlanOptions,
    pub feeds: Vec<ChildFeed>,
    /// The paths of new child tables `on_schema_change = "ignore"` leaves out, with everything below them.
    pub ignored: Vec<String>,
    /// `created ...` and `altered ...` lines.
    pub changes: Vec<String>,
    /// Under a merge, the child tables there before the load, each commit clearing the rows of its roots from them before their new rows land.
    pub merged: Vec<String>,
}

/// One child table's load: its plan from the rows so far, and its publisher once it has rows.
#[derive(Debug)]
pub struct ChildFeed {
    /// The dotted path of the list field it holds the items of.
    pub path: String,
    pub table: TablePlan,
    /// The schema merged from every batch so far, all-NULL columns as `Null`.
    pub merged: arrow_schema::Schema,
    pub publisher: Option<Publisher>,
}

/// A files target table's load: its folder, what the load replaces, and the files of the commit being written.
#[derive(Debug)]
pub struct FilesSink {
    pub output: FilesOutput,
    pub folder: OpenFolder,
    pub replaced: ReplacedFiles,
    pub writer: Option<FilesWriter>,
    /// Replaced files already dealt with by earlier commits: deleted, or overwritten by a file of the same name (`single_file`).
    pub deleted: std::collections::BTreeSet<String>,
    /// Source files (of a files source) read since the last commit, which the next commit records.
    pub reading: Vec<LoadedFile>,
    /// The load's claim on the table, refreshed in the background until `keeper`'s guard is dropped; it lives in the table's own folder, not a staged one.
    pub lease: crate::table_lease::models::Lease,
    /// The guard stopping the lease's renewal, and the renewal task.
    pub keeper: Option<(tokio_util::sync::DropGuard, tokio::task::JoinHandle<()>)>,
    /// Set when the load writes the whole table into a hidden folder beside it (`folder`) and swaps it in at commit.
    pub swap: Option<FolderSwap>,
}

/// The table folder a staged table replaces, and the files of it the load replaces; every other file, the lease among them, is carried over.
#[derive(Debug)]
pub struct FolderSwap {
    pub live: OpenFolder,
    pub replaced: std::collections::BTreeSet<String>,
}

/// A connected source, ready to read.
#[derive(Debug)]
pub enum SourceReader {
    /// The client, and the connections that help it read a large table in parallel.
    Postgres(tokio_postgres::Client, Vec<tokio_postgres::Client>),
    Files(OpenFolder),
    /// A warehouse source that unloads its tables.
    Snowflake(Box<SnowflakeReader>),
    /// BigQuery tables and queries, read through the Storage Read API.
    BigQuery(Box<crate::bigquery_source::models::BigQueryReader>),
    /// A database read through its ODBC driver.
    #[cfg(feature = "odbc")]
    Odbc(Box<crate::odbc_source::models::OdbcReader>),
    /// A Google Sheets client and the spreadsheet it reads.
    Sheets(crate::sheets_source::models::SheetsClient, String),
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

/// What `sqlload logged-state` does to the selected logged tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateKind {
    Show,
    Verify,
    Reconcile,
    Reset,
}

/// A `sqlload logged-state` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequest {
    pub kind: StateKind,
    /// Append the events (`--yes`) instead of only showing them.
    pub append: bool,
    /// The reset position (`--to`); `None` clears it.
    pub to: Option<String>,
}

/// One entry of a plan section: an object, a table load or a finding, with its labelled details.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PlanItem {
    pub name: String,
    /// What happens to it, printed after the name; empty for findings.
    pub action: String,
    pub details: Vec<PlanDetail>,
}

/// One line under a plan item: a short label (empty for a plain line) and its text, which may span lines.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PlanDetail {
    pub label: String,
    pub text: String,
}

/// A plan in sections, as `plan` prints it and `plan --json` writes it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct PlanView {
    /// Whether every selected table can load.
    pub ok: bool,
    /// Whether the plan failed only in ways a rerun may get past, such as a source that could not be reached.
    pub retryable: bool,
    /// The counts after `Plan ready`, nonzero clauses only.
    pub summary: String,
    pub selected: usize,
    pub objects: Vec<PlanItem>,
    pub loads: Vec<PlanItem>,
    pub skipped: Vec<PlanItem>,
    pub errors: Vec<PlanItem>,
    pub warnings: Vec<PlanItem>,
}

/// One line of the tally `apply` and `load` end with: `created`, `altered`, `loaded` or `failed`, the object, and what happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TallyLine {
    pub verb: &'static str,
    pub name: String,
    pub detail: String,
}

/// A files table's columns as its files hold them, for `contract diff`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilesColumns {
    /// The table's folder.
    pub folder: String,
    /// The newest load's merged schema, as `plan` reads it; `None` before any file exists.
    pub live: Option<Vec<LiveColumn>>,
    /// Why the folder or the newest load's schema cannot be read.
    pub problems: Vec<String>,
    /// Every data file's own columns, oldest partition first; only when every footer was asked for.
    pub files: Vec<FileSchema>,
}

/// One data file's columns, from its footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSchema {
    /// The key below the table's folder.
    pub key: String,
    pub columns: Vec<LiveColumn>,
}

/// An `apply` or `load` to report as JSON: what it applied and loaded, and when it started and how long it took.
#[derive(Debug, Clone, Copy)]
pub struct RunReport<'a> {
    /// `apply` or `load`.
    pub command: &'static str,
    /// The `created ...` and `altered ...` lines `apply` returned.
    pub applied: &'a [String],
    pub outcomes: &'a [TableOutcome],
    pub started: chrono::DateTime<chrono::Utc>,
    pub elapsed: std::time::Duration,
}

/// What `apply --json` and `load --json` print, in this field order.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct RunResult {
    pub schema_version: u32,
    /// `apply` or `load`.
    pub command: &'static str,
    /// Whether every selected table planned, and every table loaded.
    pub ok: bool,
    /// Whether the run failed only in ways that running it again unchanged may fix: every selected table planned, and each failed table's failure is retryable.
    pub retryable: bool,
    pub started_at: String,
    pub finished_at: String,
    pub duration_seconds: f64,
    pub summary: RunSummary,
    pub plan: PlanView,
    /// Objects created, altered or recreated, by `apply` and by Python tables' loads.
    pub objects: Vec<ObjectChange>,
    pub tables: Vec<TableResult>,
}

/// A run's counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct RunSummary {
    pub created: usize,
    pub altered: usize,
    pub recreated: usize,
    pub loaded: usize,
    pub failed: usize,
    /// Tables `--fail-fast` never started.
    pub skipped: usize,
    pub rows: u64,
}

/// An object a run created, altered or recreated.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ObjectChange {
    pub action: &'static str,
    pub name: String,
    pub detail: String,
}

/// A table's load in a run's result.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TableResult {
    pub table: String,
    pub source: Option<String>,
    pub target: Option<String>,
    pub write_strategy: Option<String>,
    /// `loaded` or `failed`.
    pub status: &'static str,
    pub rows: Option<u64>,
    pub duration_seconds: f64,
    pub note: Option<String>,
    pub error: Option<String>,
    /// Whether the failure may pass when run again unchanged; `false` once loaded.
    pub retryable: bool,
    /// What a files source's `after_load` did with the loaded files; left out when it keeps them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_load: Option<AfterLoadReport>,
}

/// Why a source's tables could not be listed, and whether a rerun may get past it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceProblem {
    pub message: String,
    pub retryable: bool,
}

impl From<String> for SourceProblem {
    fn from(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}

impl From<crate::loader::errors::LoaderError> for SourceProblem {
    fn from(error: crate::loader::errors::LoaderError) -> Self {
        Self {
            retryable: error.is_retryable(),
            message: error.to_string(),
        }
    }
}
