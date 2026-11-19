use crate::adapters::contract::constants::UNLOADED_BATCH_ROWS;
use crate::adapters::contract::models::BoundParameter;
use crate::adapters::contract::models::ChangeApply;
use crate::batch_plan::constants::MAX_WINDOWS;
use crate::column_parse::models::ColumnParse;
use crate::load_events::models::PhaseTiming;
use crate::load_naming::models::{Compat, LoadedNames, NameMemo, NameRules, Naming};
use crate::row_batches::models::{Flatten, Unnest};
use std::collections::{BTreeMap, BTreeSet};
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
    AfterLoad, AfterLoadReport, FilesEncoding, FilesOrder, FilesRead, FilesTable, Footer,
    LoadedFile, PathValues, TextChecks,
};
use crate::loader::_helpers::render::problem_item;
use crate::loader::_helpers::sources::PLANNED_SOURCES;
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{
    BatchStep, Contract, Deletes, NewTables, NullCursor, NullKeys, OnCursorOutOfRange,
    OnDuplicateKeys, OnOverlappingFiles, OnSchemaChange, OutputEncoding, Policy, ProjectConfig,
    SendMode, TableChecks, WriteStrategy,
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
}

impl std::fmt::Display for SettingOrigin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Table => "table",
            Self::Source => "source",
        })
    }
}

/// An incremental cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorSpec {
    pub column: String,
    pub start: Option<String>,
    /// A duration `lookback`, on a date or timestamp cursor.
    pub lookback: Option<Duration>,
    /// An integer `lookback`, in cursor units, on an integer cursor.
    pub lookback_units: Option<i64>,
    /// Read only cursor values before now plus this (`lookahead`).
    pub lookahead: Option<Duration>,
    /// `lookback_from = "now"`: incremental runs read from now minus `lookback`, not from the destination's maximum minus it.
    pub from_now: bool,
    /// `append_cursor_inclusive`: `append` reads from the destination's maximum itself (true) rather than after it; a first run reads from `cursor_start` inclusively either way.
    pub inclusive: bool,
    /// `cursor_bounds = "literals"`: an ODBC source writes the bounds into the query as escapes instead of binding them.
    pub literals: bool,
    /// The further columns of a coalesced cursor (`cursor_column = ["updated_at", "created_at"]`), whose value is the first non-NULL of `column` and these; empty for one column.
    pub coalesce: Vec<String>,
    /// What an incremental read does with source rows whose cursor is NULL.
    pub null_cursor: NullCursor,
    /// What a Python table's load does with the rows its function returns outside the window it was asked for.
    pub out_of_range: OnCursorOutOfRange,
}

impl CursorSpec {
    /// Every column of the cursor, `column` first.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.column.as_str()).chain(self.coalesce.iter().map(String::as_str))
    }

    /// Whether `name` is one of the cursor's columns, ignoring case.
    pub fn names(&self, name: &str) -> bool {
        self.columns()
            .any(|column| column.eq_ignore_ascii_case(name))
    }

    /// The cursor as messages and `plan` name it: its column, or `coalesce(updated_at, created_at)`.
    pub fn label(&self) -> String {
        match self.coalesce.is_empty() {
            true => self.column.clone(),
            false => format!(
                "coalesce({})",
                self.columns().collect::<Vec<_>>().join(", ")
            ),
        }
    }
}

/// How a source filter writes a cursor's text, date and timestamp bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorBinding {
    /// As literals of the source's dialect.
    Literals,
    /// As `?` placeholders bound in order, a timestamp's with its column's fractional digits when the source reports them (an ODBC source's default).
    Parameters(Option<u16>),
    /// As ODBC escapes, a timestamp's fraction at its column's digits when the source reports them (`cursor_bounds = "literals"`).
    Escapes(Option<u16>),
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
    /// Whether `unique_key` is the primary key the source reports, set as `"{primary_key}"`.
    pub key_from_primary_key: bool,
    /// Where the table's settings are written in the project file, as `sqlload.toml:93`: its `[sources.*.tables]` entry, else its source's section.
    pub settings_at: Option<String>,
    /// Where its `unique_key` is written in the project file, as `sqlload.toml:46`.
    pub unique_key_at: Option<String>,
    /// What a merge does with rows of one commit sharing `unique_key`.
    pub on_duplicate_keys: OnDuplicateKeys,
    /// What a merge does with a NULL in a `unique_key` column, and `delete_insert` in a `replace_key` column.
    pub null_keys: NullKeys,
    /// The key columns the table matches rows on (a merge's `unique_key`, a `delete_insert`'s `replace_key`) whose values are never NULL, which compare with `=`: those the source declares NOT NULL, or every one under `null_keys = "fail"`, which refuses a commit holding a NULL in them.
    pub not_null_keys: Vec<String>,
    /// Under `delete_insert`, the columns whose values each commit replaces whole: the rows holding a value of them that the commit's rows hold are deleted first. Empty otherwise.
    pub replace_key: Vec<String>,
    /// What a files source's `delete_insert` by `replace_key` does when two files of its folder hold one key value.
    pub on_overlapping_files: OnOverlappingFiles,
    /// The columns, compared in order, whose higher values are a row's newer version (`newest_by`): a merge keeps the newest of the rows sharing a key and never overwrites a loaded row with an older one; empty, the last row read wins.
    pub newest_by: Vec<String>,
    pub cursor: Option<CursorSpec>,
    /// Read and commit the cursor range one window of this size at a time.
    pub batch: Option<BatchStep>,
    /// `batch_concurrency`: the most windows read at once, still committed in order; unset reads one at a time.
    pub batch_concurrency: Option<std::num::NonZeroUsize>,
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
    /// `infer_types = false`: columns a files, Sheets or Python dict source would type by inference load as `text` instead.
    pub infer_types: bool,
    /// The target's `json_type = "variant"`: `json` columns are created as DuckDB `VARIANT` and load their values parsed.
    pub variant_json: bool,
    /// Columns pinned `native("VARIANT")` on a DuckDB or MotherDuck target: read as `json` (kept in `pinned` as `json`), loaded as `VARIANT`.
    pub variants: Vec<String>,
    pub type_change_rebuild: Policy,
    /// Whether a Python table's `replace` may empty the table when its function yields no rows.
    pub allow_empty: bool,
    /// Whether the table's position is kept in the run log (`state = "logged"`).
    pub logged: bool,
    /// What a files source does with each file once its rows commit.
    pub after_load: AfterLoad,
    /// The order a files source's merge applies its files in.
    pub files_order: FilesOrder,
    /// Approved for loading as soon as it appears: named exactly in `include`, or declared (files and Python tables); a `[sources.*.tables]` entry alone never approves.
    pub approved: bool,
    pub new_tables: NewTables,
    /// The files behind a files source; `None` for database tables.
    pub files: Option<FilesSource>,
    /// The table's folder template in a files target; `None` for warehouse targets.
    pub destination_path: Option<String>,
    /// The objects whose keys load as `<object>__<key>` columns.
    pub flatten: Flatten,
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
    /// What a merge does with rows whose key the source no longer holds; `None` without `deletes`.
    pub deletes: Option<DeletesSpec>,
    /// The table (its key under `tables`) whose `deletes_from` this table loads the tombstones of; `None` for other tables.
    pub tombstones_of: Option<String>,
    /// Columns the settings this table uses name that it doesn't have, which block it when it is planned: see `unknown_setting_columns`.
    pub setting_problems: Vec<String>,
    /// The bastion a Postgres source is reached through, as `ssh user@host:port`.
    pub tunnel: Option<String>,
    /// The checks its staged rows must pass before they are published (the table's own, else its source's).
    pub checks: Option<TableChecks>,
    /// How a warehouse source's table is read; `None` for other sources.
    pub warehouse_source: Option<WarehouseSource>,
    /// The condition every read of the table is narrowed by (`where`, its own else its source's), in the source database's SQL; `None` without one, and for query tables and tombstone tables.
    pub row_filter: Option<String>,
    /// How the table is laid out when it is created, where the warehouse takes a layout (ClickHouse).
    pub layout: TableLayout,
    /// How the table's and its columns' names load.
    pub naming: Naming,
    /// Whose names the columns the load adds take.
    pub compat: Compat,
    /// When its rows reach the target: at each commit, held in local files until then, or each batch as it is read.
    pub send: SendMode,
    /// Where `send = "at_commit"` holds a commit's rows; the system's temporary folder when `None`.
    pub staging_dir: Option<std::path::PathBuf>,
    /// Each source column's name through the name pipeline (none for a Python table, whose columns are named as they arrive), and the names numbering gave.
    pub names: LoadedNames,
    /// What the name pipeline ran under.
    pub name_rules: NameRules,
    /// The table as its source listed it, before the name pipeline, so the plan can name its columns again keeping the numbers its earlier loads gave; `None` for a Python table.
    pub listed: Option<Box<SourceTable>>,
    /// The Python function each file's (or Sheets range's) rows go through before they load; `None` without `transform`.
    pub transform: Option<TableTransform>,
    /// The text columns read as times, by the name each loads as.
    pub parses: BTreeMap<String, ColumnParse>,
    /// The zone of the times without an offset that a parse without its own `tz` reads, and whose setting it is; `None` reads them as UTC.
    pub timezone: Option<(chrono_tz::Tz, SettingOrigin)>,
}

impl TableSpec {
    /// Whether the table keeps the rows it marks deleted, with `_sqlload_deleted_at` set (`deletes = "soft"` on a merge).
    pub fn soft_deletes(&self) -> bool {
        self.deletes
            .as_ref()
            .is_some_and(|deletes| deletes.apply == Deletes::Soft)
    }

    /// The most cursor windows the table's read may be split into: fewer for a warehouse table unloaded to files, whose merge row numbers start at the window's number times [`UNLOADED_BATCH_ROWS`] in a 64-bit integer.
    pub fn most_windows(&self) -> u64 {
        match self.warehouse_source.as_ref().map(|source| &source.route) {
            Some(WarehouseRoute::Unload { .. }) => (i64::MAX / UNLOADED_BATCH_ROWS).unsigned_abs(),
            _ => MAX_WINDOWS,
        }
    }

    /// The query whose rows are the table, for a warehouse source's query table.
    pub fn query_sql(&self) -> Option<&str> {
        self.warehouse_source
            .as_ref()
            .and_then(|source| source.sql.as_deref())
    }

    /// `filter` narrowed by the table's `where`: `(<where>) AND (<filter>)`, or either alone; the `where` closes on a line of its own, so a trailing `--` comment in it ends first.
    pub fn read_filter(&self, filter: Option<String>) -> Option<String> {
        match (&self.row_filter, filter) {
            (Some(row_filter), Some(filter)) => Some(format!("({row_filter}\n) AND ({filter})")),
            (Some(row_filter), None) => Some(format!("({row_filter}\n)")),
            (None, filter) => filter,
        }
    }

    /// The zone the table's times without an offset are in: its `timezone`, else UTC.
    pub fn zone(&self) -> chrono_tz::Tz {
        self.timezone.map_or(chrono_tz::Tz::UTC, |(zone, _)| zone)
    }

    /// The type column `name` of `column_type` gets in the table: `native("VARIANT")` for `json` under the target's `json_type = "variant"` or a `native("VARIANT")` pin, else the type itself.
    pub fn loaded_type(&self, name: &str, column_type: ColumnType) -> ColumnType {
        match column_type {
            ColumnType::Json
                if self.variant_json || self.variants.iter().any(|pin| pin == name) =>
            {
                ColumnType::Native(crate::column_types::constants::VARIANT_TYPE.to_owned())
            }
            other => other,
        }
    }
}

/// A table's `transform`: the Python function each file's (or Sheets range's) rows go through before they load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransform {
    /// The name `@sl.transform` registered it under.
    pub function: String,
    /// Its name, `@` and a hash of the file it is defined in.
    pub version: String,
    /// How many of the files (or Sheets ranges) the table holds loaded under another version, since it was last rewritten.
    pub older: usize,
    /// `_sqlload_runs`, where each commit records the version, does not exist yet, so `apply` creates it (a load never does, once rows committed).
    pub creates_log: bool,
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
    /// The warehouse writes Parquet into `store`, which SQLLoad reads back and removes; `stage` says why there, for `plan`. `same_connection`: the target writes through the source's own connection, so it can select the rows itself instead.
    Unload {
        store: Store,
        stage: String,
        same_connection: bool,
    },
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

/// A merge table's `deletes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletesSpec {
    pub apply: Deletes,
    /// The largest percentage of the table's rows one key scan marks: `checks.max_row_drop`, else 5.
    pub max_drop: u8,
    /// `--sync-deletes`: after its merge, the load reads every key from the source and marks the table's rows of the others.
    pub scan: bool,
    /// Its `deletes_from`: after each merge, the rows its tombstones name are marked.
    pub from: Option<TombstoneSpec>,
}

/// A merge table's tombstone table (`deletes_from`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneSpec {
    /// The tombstone table as the source names it (`audit.deleted_items`).
    pub source_table: String,
    /// Where it loads, and its `cursor_column` as loaded, once planned beside the table; `None` before.
    pub loaded: Option<(TableName, String)>,
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
    /// The `sl.http` paths its function writes as literals, which `plan` checks; `None` when it builds no job `plan` can see.
    pub literal_paths: Option<crate::http_source::models::LiteralPaths>,
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
    /// How each file's key gives its partition, path and constant values.
    pub paths: PathValues,
    pub encoding: FilesEncoding,
    /// Column names and patterns JSON and CSV reads check keys and headers against.
    pub checks: TextChecks,
    /// Notes `plan` shows on the table, such as pinned columns no sampled file has.
    pub notes: Vec<String>,
    /// `copy_to_internal_stage`: files no external stage reads are uploaded unchanged to an internal stage, and loaded from there by the warehouse.
    pub copy_to_internal_stage: bool,
    /// How each file is read for its table's `transform`; `None` without one, when `table`'s columns are the files'.
    pub transform: Option<FilesTransform>,
    /// Under `--reload` with `after_load.move_to`, the folder the loaded files were moved to, whose files `table` lists too (keeping their keys, so their names); they stay there after the load.
    pub archive: Option<Location>,
}

/// How a files table with a `transform` reads each file before the function runs; its source table's columns are then what the function returned for `sample` at plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesTransform {
    /// The columns each file is read in: as stored (Parquet) or as the workbook holds them (xlsx), JSON's nested values as JSON and every other value as text.
    pub input: Vec<DesiredColumn>,
    /// Time and timestamp columns read at nanoseconds, as the files hold them.
    pub nanos: Vec<String>,
    /// What the files' keys and headers are checked against.
    pub checks: TextChecks,
    /// The file the function ran on at plan, as `_sqlload_source_file` names it.
    pub sample: String,
}

/// A table that matched a source's selection but is not loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedTable {
    pub source_name: String,
    pub source_table: String,
    /// The name `plan` shows it by: its destination's display name, as `schema.table`.
    pub destination: String,
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
    /// The destination table, its display name and the source of each files source skipped as drained (see [`DrainedFiles`]): names a selector may use.
    pub drained: Vec<String>,
}

/// The settings for tables of one project, before any destination is read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedTables {
    pub specs: Vec<TableSpec>,
    pub skipped: Vec<SkippedTable>,
    /// Problems with one table's settings, as `source_table: problem`.
    pub problems: Vec<String>,
    /// Each of `problems` about one table, with the display name of the table it would load into.
    pub problem_names: Vec<(String, String)>,
    /// Each of `problems` about one table, with where that table's settings are written (see [`TableSpec::settings_at`]), if known.
    pub problem_places: Vec<(String, Option<String>)>,
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
    /// How an ODBC table's rows are fetched under its memory budget.
    pub fetch_memory: Option<FetchMemory>,
}

/// How an ODBC table's rows are fetched, worked out at plan on the machine that loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchMemory {
    /// The bytes one fetch may hold.
    pub budget: usize,
    /// The bytes one fetch holds at most, as planned: its bound buffers, or the budget for a table read value by value; `None` when the driver couldn't describe the columns.
    pub estimate: Option<usize>,
    /// What `plan` says about the fetches, the `fetch` line.
    pub memory: String,
    /// The fetch buffer and why, `plan`'s `buffer` line.
    pub budget_note: String,
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
    /// The commits whose files the table held when it was planned: a load stops if another load changed them since.
    pub commits: BTreeSet<String>,
}

impl FilesOutput {
    /// The unfinished loads repair has work for: all but expired `aborted` tombstones with no file left, which it only deletes.
    pub fn unfinished(&self) -> impl Iterator<Item = &PendingLoad> {
        self.pending
            .iter()
            .filter(|load| !(load.expired && load.deletes.is_empty()))
    }
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
    /// The sources with a problem in `problems`, which a table's problem names only by its `schema.table`.
    pub failed_sources: std::collections::BTreeSet<String>,
    /// Each of `problems` about one table, with the display name of the table it would load into, which `plan` names it by; the n-th of a text goes with the n-th of `problems` with that text.
    pub problem_names: Vec<(String, String)>,
    /// Each of `problems` about one table, with where that table's settings are written if known, which its `fix` starts with; paired as `problem_names` are.
    pub problem_places: Vec<(String, Option<String>)>,
    /// What each in-scope source that could not be read holds up, by source name.
    pub unread_sources: BTreeMap<String, UnreadSource>,
    /// Each warning that a source could not be read so tables may share a destination with it, and what it is about.
    pub unread_sharing: Vec<UnreadSharing>,
    /// How long each target's and source's connection and each source's discovery took, when the command line timed them.
    pub phases: Vec<PhaseTiming>,
    /// The run's memory budget and how it is shared, `plan`'s `memory` line; `None` when the command line didn't work it out.
    pub memory: Option<String>,
    /// The `memory` line asks for something, so `plan` shows it without `-v` too (see [`RunMemory::asks`](crate::memory_budget::models::RunMemory)).
    pub memory_asks: bool,
    /// One line per `http` connection: the limits every request to it shares in the run, its retries and its auth scheme.
    pub http: Vec<String>,
}

/// The tables a source that could not be read would load, as far as its settings name them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnreadSource {
    /// The display names of the tables it names itself: exactly in `include`, under `tables`, or a files source's one table.
    pub tables: Vec<String>,
    /// Whether its `include` also has patterns, whose tables are unknown.
    pub patterns: bool,
    /// The connection it reads through.
    pub connection: String,
    /// The `${ENV:NAME}` variables that connection's settings read.
    pub variables: Vec<String>,
}

/// A warning that sources could not be read, so tables may share a destination table with them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnreadSharing {
    /// The warning as `plan.warnings` holds it.
    pub warning: String,
    /// The sources that could not be read.
    pub sources: Vec<String>,
    /// The display names of the tables whose check it holds up.
    pub tables: Vec<String>,
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

    /// The source of each error the plan stops on, without repeats: a blocked table's, the source a problem names (`[sources.<name>]...`), and each source recorded with a table problem. A problem with the project as a whole or a connection names none, as only fixing it gets past it.
    pub fn failing_sources(&self) -> Vec<String> {
        let mut sources: Vec<String> = Vec::new();
        let named = self.problems.iter().filter_map(|problem| {
            let subject = problem_item(problem).name;
            let inner = subject.strip_prefix("[sources.")?;
            Some(inner.split(['.', ']']).next().unwrap_or(inner).to_owned())
        });
        let blocked = self
            .tables
            .iter()
            .filter(|table| table.schema.blocked.is_some())
            .map(|table| table.spec.source_name.clone());
        for source in blocked
            .chain(named)
            .chain(self.failed_sources.iter().cloned())
        {
            if !sources.contains(&source) {
                sources.push(source);
            }
        }
        sources
    }

    /// The sources the plan running now has finished checking, for the line `plan` shows while it works.
    pub fn planned_sources() -> usize {
        PLANNED_SOURCES.load(std::sync::atomic::Ordering::Relaxed)
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
    /// `--start-cursor` (or, for one table, its family's `-ts`/`-int`): read from this cursor value instead of the destination's maximum.
    pub start_cursor: Option<String>,
    /// `--end-cursor` (or, for one table, its family's `-ts`/`-int`): read only cursor values below this one.
    pub end_cursor: Option<String>,
    /// `--start-cursor-ts`/`--end-cursor-ts` and `--start-cursor-int`/`--end-cursor-int`: each cursor table reads the range of its own family.
    pub typed_cursors: TypedCursors,
    /// The flags `start_cursor` and `end_cursor` came from, as `plan` names them: the untyped ones, or the typed ones of the family one table's options took.
    pub range_flags: RangeFlags,
    /// `--concurrency`: the most tables loading at once, overriding the project's `concurrency`.
    pub concurrency: Option<usize>,
    /// `--memory-limit`: the run's memory budget, or `off`, overriding the project's `memory_limit`.
    pub memory_limit: Option<crate::memory_budget::models::MemorySetting>,
    /// The bytes a batch holds before it may commit, instead of about 1 GB (a hidden flag, for tests).
    pub batch_bytes: Option<usize>,
    /// `--resnapshot`: drop and recreate each selected `postgres_cdc` source's slot and copy every table again.
    pub resnapshot: bool,
    /// `--sync-deletes`: after each selected `deletes` table's merge, read every key from its source and mark its rows of the others.
    pub sync_deletes: bool,
    /// `--fail-fast`: once a table fails, start no other; tables already loading finish.
    pub fail_fast: bool,
    /// `--memory-per-table` (Linux): one table at a time, each table's peak memory measured.
    pub memory_per_table: bool,
}

/// The typed cursor range flags, as given.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypedCursors {
    pub start_ts: Option<String>,
    pub end_ts: Option<String>,
    pub start_int: Option<String>,
    pub end_int: Option<String>,
}

impl PlanOptions {
    /// Whether a cursor range is given, by `--start-cursor`/`--end-cursor` or a typed flag.
    pub fn ranged(&self) -> bool {
        let typed = &self.typed_cursors;
        [
            &self.start_cursor,
            &self.end_cursor,
            &typed.start_ts,
            &typed.end_ts,
            &typed.start_int,
            &typed.end_int,
        ]
        .iter()
        .any(|value| value.is_some())
    }
}

/// The flags a cursor range came from, as `plan` names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeFlags {
    pub start: &'static str,
    pub end: &'static str,
}

impl Default for RangeFlags {
    fn default() -> Self {
        Self {
            start: "--start-cursor",
            end: "--end-cursor",
        }
    }
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
    /// The most memory the process held while the table loaded, in bytes; only under `--memory-per-table`.
    pub peak_memory_bytes: Option<u64>,
}

impl TableOutcome {
    /// `table` as a table `--fail-fast` never started once `failed` (a table's outcome name) failed: skipped and retryable, as a rerun loads it.
    pub fn skipped(table: &TablePlan, failed: &str) -> Self {
        Self {
            table: table.outcome_name(),
            origin: Some(table.origin()),
            loaded: Err(format!(
                "not started: --fail-fast stopped the load after {failed} failed"
            )),
            retryable: true,
            elapsed: std::time::Duration::ZERO,
            skipped: true,
            peak_memory_bytes: None,
        }
    }

    /// A table the memory watchdog's run stop kept from starting.
    pub fn run_stopped(table: &TablePlan) -> Self {
        Self {
            table: table.outcome_name(),
            origin: Some(table.origin()),
            loaded: Err("not started: the memory watchdog stopped the run".to_owned()),
            retryable: false,
            elapsed: std::time::Duration::ZERO,
            skipped: true,
            peak_memory_bytes: None,
        }
    }

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
    /// The `replace_key` values a `delete_insert` replaced; `None` without a `replace_key`.
    pub replaced_keys: Option<ReplacedKeys>,
    /// How long the table's `transform` took on each file or range, in the order they loaded.
    pub transforms: Vec<crate::python_sources::models::TransformTime>,
    /// A line per column `rename` named or numbering numbered.
    pub names: Vec<String>,
    /// What the load warns of: a Python table's rows outside the cursor window it skipped or loaded.
    pub warnings: Vec<String>,
}

/// What a load's reads need to run a table's `transform`: the host, and where each call's time goes.
#[derive(Debug, Clone)]
pub struct TransformRun {
    pub host: std::sync::Arc<dyn PythonHost>,
    pub times: std::sync::Arc<std::sync::Mutex<Vec<crate::python_sources::models::TransformTime>>>,
}

impl TransformRun {
    /// A run of `host`'s transforms, no call timed yet.
    pub fn new(host: std::sync::Arc<dyn PythonHost>) -> Self {
        Self {
            host,
            times: std::sync::Arc::default(),
        }
    }
}

/// The `replace_key` values whose rows a table's `delete_insert` replaced, as `load --json` reports them.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReplacedKeys {
    /// The `replace_key` columns.
    pub columns: Vec<String>,
    /// Each value its commits replaced, in order, as its columns' text (`NULL` for a NULL) joined by `, `; at most [`REPLACED_KEYS_SHOWN`](crate::loader::constants::REPLACED_KEYS_SHOWN).
    pub values: std::collections::BTreeSet<String>,
    /// Whether more values were replaced than `values` lists.
    pub truncated: bool,
}

impl ReplacedKeys {
    /// `kept` (none before the first commit) with one commit's replaced values of `columns` added, keeping the first [`REPLACED_KEYS_SHOWN`](crate::loader::constants::REPLACED_KEYS_SHOWN) in order and marking it truncated when there are more.
    pub fn added(kept: Option<Self>, (columns, values): (Vec<String>, Vec<String>)) -> Self {
        let mut kept = kept.unwrap_or(Self {
            columns,
            ..Self::default()
        });
        kept.values.extend(values);
        while kept.values.len() > crate::loader::constants::REPLACED_KEYS_SHOWN {
            kept.values.pop_last();
            kept.truncated = true;
        }
        kept
    }
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
    /// The rows the function returned outside the current cursor window.
    pub outside: crate::batch_plan::models::OutsideRows,
    /// The load report's lines on rows outside windows (`on_cursor_out_of_range = "skip"` or `"warn"`), for the first few windows that had any.
    pub warnings: Vec<String>,
    /// The rows outside later windows, and how many windows, which no line shows.
    pub unshown: (u64, u64),
    /// Of those, the rows whose cursor was NULL, which fail the table unless `null_cursor` says otherwise.
    pub null_cursors: u64,
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
    /// The names the load gave the yielded fields so far.
    pub names: NameMemo,
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
    /// A key scan (`--sync-deletes`): its one commit marks the rows of every key it didn't read.
    KeyScan,
    /// `single_file`: each file is written once, whole.
    SingleFile,
    /// The table cannot load, so it has no commits.
    Blocked,
    /// `delete_insert` by `replace_key`: a commit deletes every row of its key values, so a value's rows must commit together.
    ReplaceKey,
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

/// A files source's merge under `--reload`: each file merges into `build`, which replaces the table once every file merged, with `records` (a reset, then one per file) in the swap's transaction, so a failed reload leaves the table and its run log as they were.
#[derive(Debug)]
pub struct MergeRebuild {
    pub build: TableName,
    pub records: Vec<RunRecord>,
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
    /// When the last `table_progress` event went out, so a long batch reports its rows as they arrive.
    pub progressed_at: std::time::Instant,
    /// A child table's publisher, filled by its parent's load, which sends no events of its own: events name only the tables `plan` names.
    pub child: bool,
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
    /// A files source's merge, or `delete_insert` by `replace_key`: the file (`name|version`) the next commit loads, recorded in the run log with it.
    pub merged_file: Option<String>,
    /// The `replace_key` values committed so far; `None` without a `replace_key`.
    pub replaced: Option<ReplacedKeys>,
    /// The rows a key scan's commit marked deleted; `None` for other loads.
    pub marked: Option<u64>,
    /// The keys whose newest staged row a merge skipped so far, the table holding a newer one by `newest_by`.
    pub older: u64,
    /// Every `replace_key` value a files source's commits inserted in this run, with the file it came from, which a later file of the run may not hold too.
    pub run_keys: std::collections::BTreeMap<String, String>,
    /// The files fed since the last commit, as they were read, settled by the next one (`after_load`).
    pub fed_files: Vec<StoredFile>,
    /// The files whose rows are committed, as they were read, which `after_load` deletes or moves.
    pub settled_files: Vec<StoredFile>,
    /// The child tables `unnest` fills from the table's rows; `None` without `unnest`.
    pub children: Option<ChildTables>,
    /// A files source's merge under `--reload`, which merges each file into a new table swapped in at the end.
    pub rebuild: Option<MergeRebuild>,
    /// The `transform`'s calls so far, and how many the commits recorded: each commit records the version with the files or ranges since.
    pub transform: Option<(TransformRun, usize)>,
    /// Under `send = "at_commit"`, the batches held locally until their commit sends them; `None` when each batch is sent as it is read.
    pub spool: Option<crate::commit_spool::models::Spool>,
    /// The commit being cut into pieces under DuckDB's memory share.
    pub pieces: Pieces,
}

/// Where a table's commit may be cut into pieces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cuts {
    /// Between any two rows.
    Anywhere,
    /// Only where these columns' value changes: a `delete_insert` replaces each value's rows together.
    Between(Vec<String>),
    /// Nowhere, and why.
    Never(&'static str),
}

/// A table's commits cut into pieces, each holding no more large values (those DuckDB keeps in memory until a checkpoint) than DuckDB's share commits at once.
#[derive(Debug, Default)]
pub struct Pieces {
    /// The large values written since the last commit, counted as many times as DuckDB holds them (see [`STAGED_COPIES`](crate::memory_budget::constants::STAGED_COPIES)).
    pub held: u64,
    /// The large values of the rows in a row since the last change of the value pieces may only be cut between, which must commit together.
    pub run: u64,
    /// That value as text, at the last row written.
    pub last_key: Option<String>,
    /// Pieces committed since the last whole commit, which spares this load's rows when it deletes (see [`WriteStatementsRequest::spared_load`](crate::adapters::contract::models::WriteStatementsRequest)).
    pub committed: u64,
    /// The recovery marker is written: the table holds this load's rows before its last commit.
    pub unfinished: bool,
    /// The commit running is a piece: it records no run and leaves the load's position as it was.
    pub piece: bool,
    /// What the load's row group checks saw (see `Publisher::check_row_group`); `None` before the first.
    pub group: Option<RowGroupSeen>,
    /// The rows of this load, and their large values, which give its rows' width.
    pub rows: u64,
    pub large: u64,
    /// How many times DuckDB holds the large values of a piece: three through a staging table, else once.
    pub copies: u64,
    /// The rows of `run` outgrew what DuckDB commits at once: the rest of them are counted, not written, and the table stops.
    pub overflow: bool,
    /// A marker statement for rows held under `send = "at_commit"`: it runs in the transaction that sends them, before they load.
    pub statement: Option<String>,
}

/// What a load's row group checks saw of the table's open row group.
#[derive(Debug, Clone, Copy, Default)]
pub struct RowGroupSeen {
    /// At the first check: the table's committed rows, the rows of its full row groups, and the bytes of the strings in the rest.
    pub before: u64,
    pub full: u64,
    pub earlier: u64,
    /// The row groups the load has filled since, and its large values when the last of them filled, which belong to the row groups before.
    pub filled: u64,
    pub large_at: u64,
    /// The load's large values at the last check.
    pub last_large: u64,
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
    /// An open handle on the staged folder, which the swap checks is still the folder this load made.
    pub held: std::fs::File,
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
    /// Databricks tables and queries, read as Arrow through the Statement Execution API.
    Databricks(Box<crate::databricks_source::models::DatabricksReader>),
    /// A database read through its ODBC driver.
    #[cfg(feature = "odbc")]
    Odbc(Box<crate::odbc_source::models::OdbcReader>),
    /// A DuckDB database file, opened read-only for each read.
    #[cfg(feature = "duckdb")]
    DuckDb(Box<crate::duckdb_source::models::DuckDbReader>),
    /// A Google Sheets client and the spreadsheet it reads.
    Sheets(crate::sheets_source::models::SheetsClient, String),
}

/// Where a files table's window can start and end: `--start-cursor`, `--end-cursor` and `cursor_start` as cursor text, and the value of the target's newest partition (none under `--reload`).
#[derive(Debug)]
pub struct FilesWindowStarts<'a> {
    pub start: Option<String>,
    pub end: Option<String>,
    pub cursor_start: Option<String>,
    pub newest: Option<&'a str>,
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
    /// `plan`'s `to` lines: each destination the tables load into, as `Snowflake analytics`.
    #[serde(skip)]
    pub destinations: Vec<String>,
    pub objects: Vec<PlanItem>,
    pub loads: Vec<PlanItem>,
    pub skipped: Vec<PlanItem>,
    pub errors: Vec<PlanItem>,
    pub warnings: Vec<PlanItem>,
    /// How long each connection and discovery took before the plan.
    pub phases: Vec<PhaseTiming>,
    /// The run's memory budget and how it is shared.
    pub memory: Option<String>,
    /// Whether `plan` shows `memory` without `-v`: it asks for something.
    #[serde(skip)]
    pub memory_asks: bool,
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
    /// The run's peak memory, in bytes, when the OS gives it.
    pub peak_memory_bytes: Option<u64>,
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
    /// The most memory the whole process held at once during the run, in bytes; left out where the OS gives no figure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_memory_bytes: Option<u64>,
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
    /// The `replace_key` values its `delete_insert` replaced; left out without a `replace_key`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaced_keys: Option<ReplacedKeys>,
    /// How long the table's `transform` took on each file or range; left out without one.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transforms: Vec<crate::python_sources::models::TransformTime>,
    /// A line per column `rename` named or numbering numbered; left out when there is none.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    /// What the load warns of, such as rows outside a cursor window; left out when there is nothing.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// The most memory the process held while the table loaded, in bytes; only under `--memory-per-table`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_memory_bytes: Option<u64>,
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
