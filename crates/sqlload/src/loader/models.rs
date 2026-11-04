use std::collections::BTreeMap;
use std::time::Duration;

use crate::column_types::models::{ColumnType, Warehouse};
#[cfg(feature = "duckdb")]
use crate::duckdb_destination::models::DuckDbDestination;
use crate::file_store::models::OpenFolder;
use crate::files_destination::models::{FilesDestination, PartitionFolder, TableFiles, TablePath};
use crate::files_source::models::{FilesRead, FilesTable, Footer};
use crate::masking::models::MaskRules;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{
    Contract, FilesFormat, NewTables, OnSchemaChange, Policy, WriteStrategy,
};
use crate::routing::models::Route;
use crate::schema_plan::models::{LiveColumn, SchemaPlan};
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
    /// Listed under `[sources.*.tables]`, so never treated as a new table.
    pub listed: bool,
    pub new_tables: NewTables,
    /// The files behind a files source; `None` for database tables.
    pub files: Option<FilesSource>,
    /// The table's folder template in a files target; `None` for warehouse targets.
    pub destination_path: Option<String>,
}

/// Where a files source's files are and what they hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesSource {
    pub location: Location,
    pub table: FilesTable,
    /// Declared partition columns and their types, by name; a file column of the same name is replaced by the path's value.
    pub partition_types: Vec<(String, ColumnType)>,
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
    /// How the data moves from the source to the destination.
    pub route: Option<Route>,
    /// The files to read, for a files source.
    pub files: Option<FilesRead>,
    /// Where and how the files are written, for a files target.
    pub output: Option<FilesOutput>,
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

/// What one table's load did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadResult {
    pub table: String,
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
pub enum SourceReader {
    Postgres(tokio_postgres::Client),
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

/// A files table as it is now: its files, and its newest partition's footers and schema.
#[derive(Debug, Clone)]
pub struct LiveFiles {
    pub listed: TableFiles,
    pub footers: Vec<Footer>,
    /// The merged schema of the newest partition's files; `None` before any file exists.
    pub live: Option<Vec<LiveColumn>>,
}
