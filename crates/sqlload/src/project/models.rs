use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::config::models::AuthConfig;
use crate::files_source::models::{Compression, CsvOptions, FilesEncoding, SourceFormat};
use crate::project::_helpers::defaults;
use crate::project::_helpers::serde_values::{
    deserialize_optional_duration, deserialize_optional_keys,
};
use crate::project::constants::{
    DEFAULT_DESTINATION_PATH, DEFAULT_FILES_TABLE_NAME, DEFAULT_MAX_FOOTER_READS,
    DEFAULT_TABLE_NAME,
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
    /// Every connection's settings after `${NAME}` interpolation, for Python table functions.
    #[serde(skip)]
    pub connection_settings: serde_json::Map<String, serde_json::Value>,
    /// The file declaring each Python source, by source name.
    #[serde(skip)]
    pub python_modules: BTreeMap<String, String>,
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
    /// An S3 bucket; credentials come from `AWS_*` environment variables, web identity, or the instance/container role (AWS profiles and SSO are not read yet).
    S3 {
        bucket: String,
        #[serde(default)]
        region: Option<String>,
        /// A custom endpoint such as MinIO.
        #[serde(default)]
        endpoint: Option<String>,
    },
    /// A directory on the machine SQLLoad runs on.
    Filesystem { root: String },
    #[serde(rename = "duckdb")]
    DuckDb {
        /// The database file; `:memory:` for an in-memory database.
        path: String,
    },
    /// An HTTP API, for Python sources; its settings reach table functions as `ctx.connection`.
    Http {
        base_url: String,
        #[serde(default)]
        token: Option<crate::auth::models::Secret>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

impl ConnectionConfig {
    /// The `kind` value as written.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Snowflake { .. } => "snowflake",
            Self::Postgres { .. } => "postgres",
            Self::DuckDb { .. } => "duckdb",
            Self::S3 { .. } => "s3",
            Self::Filesystem { .. } => "filesystem",
            Self::Http { .. } => "http",
        }
    }
}

/// Where loaded tables go in one environment: a warehouse, or files in an s3 or filesystem connection.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub connection: String,
    /// Overrides the connection's database (warehouse targets).
    #[serde(default)]
    pub database: Option<String>,
    /// The schema for tables whose source sets none; required for warehouse targets, rejected for files targets.
    #[serde(default)]
    pub loader_schema: Option<String>,
    /// The folder of each table, a template with `{source}`, `{schema}`, `{table}` and `{cursor}` or `{cursor:date}` (files targets).
    #[serde(default)]
    pub path: Option<String>,
    /// The format of the files written (files targets).
    #[serde(default)]
    pub format: Option<FilesFormat>,
    /// The most Parquet footers `plan` reads for one table's schema (files targets).
    #[serde(default)]
    pub max_footer_reads: Option<usize>,
}

impl TargetConfig {
    /// The table folder template: as set, or `{source}/{table}/`.
    pub fn path_template(&self) -> &str {
        self.path.as_deref().unwrap_or(DEFAULT_DESTINATION_PATH)
    }

    /// The footer read cap: as set, or the default.
    pub fn footer_read_limit(&self) -> usize {
        self.max_footer_reads.unwrap_or(DEFAULT_MAX_FOOTER_READS)
    }
}

/// A declarative source: where data comes from, and what its tables share (unknown keys are checked by the parser).
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub kind: SourceKind,
    /// The connection the source reads; optional for Python sources.
    #[serde(default)]
    pub connection: String,
    #[serde(default)]
    pub description: Option<String>,
    /// The target to load into; `default_target` when omitted.
    #[serde(default)]
    pub destination: Option<String>,
    /// The destination schema; the target's `loader_schema` when omitted.
    #[serde(default)]
    pub schema: Option<String>,
    /// Source tables to sync, as `schema.table` patterns (database sources).
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub new_tables: NewTables,
    /// The destination table name, with `{source}`, `{schema}` and `{table}`; see [`SourceConfig::table_name_template`].
    #[serde(default)]
    pub table_name: Option<String>,
    /// The folder holding the files, relative to the connection (files sources).
    #[serde(default)]
    pub path: Option<String>,
    /// The files' format (files sources).
    #[serde(default)]
    pub format: SourceFormat,
    /// How JSON and CSV files are compressed; by file name (`.gz`, `.zst`) when omitted (files sources).
    #[serde(default)]
    pub compression: Option<Compression>,
    /// How CSV files are written (files sources with `format = "csv"`).
    #[serde(default)]
    pub csv: Option<CsvOptions>,
    /// The folder template for this source's tables in a files target, overriding the target's `path`.
    #[serde(default)]
    pub destination_path: Option<String>,
    /// Hive `key=value` folders loaded as typed columns (files sources).
    #[serde(default)]
    pub partition_columns: BTreeMap<String, String>,
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    #[serde(default)]
    pub skip_tables_with_columns: Vec<String>,
    #[serde(default)]
    pub allow_columns: Vec<String>,
    /// The key for `mask = { col = "hmac" }`, normally `${ENV}`.
    #[serde(default)]
    pub mask_key: Option<crate::auth::models::Secret>,
    /// Defaults for every table of the source.
    #[serde(flatten)]
    pub defaults: TableSettings,
    /// Per-table settings, keyed by `schema.table`.
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverride>,
}

impl SourceConfig {
    /// How a files source's files are encoded, with defaults for what is not set.
    pub fn files_encoding(&self) -> FilesEncoding {
        FilesEncoding {
            format: self.format,
            compression: self.compression.unwrap_or_default(),
            csv: self.csv.clone().unwrap_or_default(),
        }
    }

    /// The destination table name template: as set, or `{source}__{table}` for databases and Python sources and `{source}` for files.
    pub fn table_name_template(&self) -> &str {
        match (&self.table_name, self.kind) {
            (Some(template), _) => template,
            (None, SourceKind::Postgres | SourceKind::Python) => DEFAULT_TABLE_NAME,
            (None, SourceKind::Files) => DEFAULT_FILES_TABLE_NAME,
        }
    }
}

/// The declarative source kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Postgres,
    /// Existing files in an object store or directory.
    Files,
    /// Declared in Python with `sl.source` or `@sl.table`; never written in `sqlload.toml`.
    Python,
}

/// The file formats a files target writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesFormat {
    #[default]
    Parquet,
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
    /// Let a Python table's `replace` (or `--reload`) empty the table when its function yields no rows.
    #[serde(default)]
    pub allow_empty: Option<bool>,
}

/// One table's settings: everything a source can set, plus what only a table can.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TableOverride {
    #[serde(flatten)]
    pub settings: TableSettings,
    #[serde(default)]
    pub description: Option<String>,
    /// The destination table name, overriding the source's `table_name`; `{source}`, `{schema}` and `{table}` are filled in when it has them.
    #[serde(default)]
    pub name: Option<String>,
    /// The folder template in a files target, overriding the source's `destination_path`.
    #[serde(default)]
    pub destination_path: Option<String>,
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
    /// The lowercase hex SHA-256 of the value's text; anyone can recompute it, so guessable values can be recovered.
    Hash,
    /// The lowercase hex HMAC-SHA256 of the value's text, keyed by the source's `mask_key`.
    Hmac,
}

impl std::fmt::Display for MaskMethod {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Hash => "hash",
            Self::Hmac => "hmac",
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
