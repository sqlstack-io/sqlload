use std::collections::BTreeMap;
use std::time::Duration;

use crate::column_types::models::{ColumnType, Warehouse};
use crate::duckdb_destination::models::DuckDbDestination;
use crate::postgres_source::models::{ReadColumn, SourceTable};
use crate::project::models::{
    Contract, MaskMethod, NewTables, OnSchemaChange, Policy, WriteStrategy,
};
use crate::schema_plan::models::{LiveColumn, SchemaPlan};
use crate::snowflake_destination::models::SnowflakeDestination;
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
    /// Columns loaded masked, read as text.
    pub masked: BTreeMap<String, MaskMethod>,
    pub contract: Contract,
    pub on_schema_change: OnSchemaChange,
    pub type_change_rebuild: Policy,
    /// Listed under `[sources.*.tables]`, so never treated as a new table.
    pub listed: bool,
    pub new_tables: NewTables,
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
    DuckDb(DuckDbDestination),
    Snowflake(Box<SnowflakeDestination>),
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
