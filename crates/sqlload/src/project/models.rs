use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::config::models::AuthConfig;
use crate::project::_helpers::defaults;
use crate::project::_helpers::serde_values::{
    deserialize_optional_duration, deserialize_optional_keys,
};

/// A loader project: `sqlload.toml` with `[connections]`, `[targets]` and `[sources]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    /// Folders scanned for Python sources; none when omitted.
    #[serde(default)]
    pub python_paths: Vec<PathBuf>,
    /// The target used when a source names none.
    #[serde(default)]
    pub default_target: Option<String>,
    #[serde(default)]
    pub connections: BTreeMap<String, ConnectionConfig>,
    #[serde(default)]
    pub targets: BTreeMap<String, TargetConfig>,
    #[serde(default)]
    pub sources: BTreeMap<String, SourceConfig>,
}

/// How to reach one system.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConnectionConfig {
    Snowflake {
        account: String,
        user: String,
        auth: AuthConfig,
        #[serde(default)]
        role: Option<String>,
        #[serde(default)]
        warehouse: Option<String>,
        database: String,
        /// Overrides the account URL, for tests.
        #[serde(default)]
        base_url: Option<String>,
    },
    Postgres {
        host: String,
        #[serde(default = "defaults::postgres_port")]
        port: u16,
        user: String,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
        database: String,
    },
    #[serde(rename = "duckdb")]
    DuckDb {
        /// The database file; `:memory:` for an in-memory database.
        path: String,
    },
}

impl ConnectionConfig {
    /// The `kind` value as written.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Snowflake { .. } => "snowflake",
            Self::Postgres { .. } => "postgres",
            Self::DuckDb { .. } => "duckdb",
        }
    }
}

/// Where loaded tables go in one environment.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub connection: String,
    /// Overrides the connection's database.
    #[serde(default)]
    pub database: Option<String>,
    /// The schema for tables whose source sets none.
    pub loader_schema: String,
}

/// A declarative source: where data comes from, and what its tables share (unknown keys are checked by the parser).
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub kind: SourceKind,
    pub connection: String,
    #[serde(default)]
    pub description: Option<String>,
    /// The target to load into; `default_target` when omitted.
    #[serde(default)]
    pub destination: Option<String>,
    /// The destination schema; the target's `loader_schema` when omitted.
    #[serde(default)]
    pub schema: Option<String>,
    /// Source tables to sync, as `schema.table` patterns.
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub new_tables: NewTables,
    /// The destination table name, with `{source}`, `{schema}` and `{table}`.
    #[serde(default = "defaults::table_name")]
    pub table_name: String,
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    #[serde(default)]
    pub skip_tables_with_columns: Vec<String>,
    #[serde(default)]
    pub allow_columns: Vec<String>,
    /// Defaults for every table of the source.
    #[serde(flatten)]
    pub defaults: TableSettings,
    /// Per-table settings, keyed by `schema.table`.
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverride>,
}

/// The declarative source kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Postgres,
}

/// What to do with source tables that match `include` but were not seen before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NewTables {
    #[default]
    Sync,
    Skip,
    Fail,
}

/// Settings a source gives all its tables and a table may override; every one optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct TableSettings {
    #[serde(default)]
    pub write_strategy: Option<WriteStrategy>,
    #[serde(default, deserialize_with = "deserialize_optional_keys")]
    pub unique_key: Option<Vec<String>>,
    #[serde(default)]
    pub cursor_column: Option<String>,
    #[serde(default)]
    pub cursor_start: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub lookback: Option<Duration>,
    #[serde(default)]
    pub append_cursor_inclusive: Option<bool>,
    #[serde(default)]
    pub contract: Option<Contract>,
    #[serde(default)]
    pub on_schema_change: Option<OnSchemaChange>,
    #[serde(default)]
    pub auto_widen: Option<bool>,
    #[serde(default)]
    pub type_change_rebuild: Option<Policy>,
}

/// One table's settings: everything a source can set, plus what only a table can.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TableOverride {
    #[serde(flatten)]
    pub settings: TableSettings,
    #[serde(default)]
    pub description: Option<String>,
    /// The destination table name, overriding the source's `table_name`.
    #[serde(default)]
    pub name: Option<String>,
    /// Pinned column types, by column name.
    #[serde(default)]
    pub columns: BTreeMap<String, String>,
    /// Columns kept but masked, by column name; wins over `exclude_columns`.
    #[serde(default)]
    pub mask: BTreeMap<String, MaskMethod>,
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    #[serde(default)]
    pub allow_columns: Vec<String>,
}

/// How a masked column's values are replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskMethod {
    /// The lowercase hex SHA-256 of the value's text.
    Hash,
}

impl std::fmt::Display for MaskMethod {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Hash => "hash",
        })
    }
}

/// How rows are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteStrategy {
    Append,
    Merge,
    DeleteInsert,
    Replace,
}

impl std::fmt::Display for WriteStrategy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Append => "append",
            Self::Merge => "merge",
            Self::DeleteInsert => "delete_insert",
            Self::Replace => "replace",
        })
    }
}

/// Where a table's column definitions come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Contract {
    /// The live table is the contract.
    #[default]
    None,
    /// The declared columns are the contract.
    Enforced,
}

/// What a new source field does to the destination table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    #[default]
    AppendNewColumns,
    Ignore,
    Fail,
}

/// Whether a costly change may run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    Allow,
    #[default]
    RequireConfirmation,
    Deny,
}
