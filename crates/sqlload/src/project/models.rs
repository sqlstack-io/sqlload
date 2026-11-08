use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::config::models::AuthConfig;
use crate::files_source::constants::RECORD_COLUMN;
use crate::files_source::models::{
    Compression, CsvOptions, FilesEncoding, RecordShape, SourceFormat, UnmatchedPaths,
};
use crate::project::_helpers::defaults;
use crate::project::_helpers::serde_values::{
    deserialize_optional_batch, deserialize_optional_duration, deserialize_optional_keys,
    deserialize_optional_percent,
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
    /// The most tables loading at once (each table's batches still commit one at a time); `--concurrency` overrides it.
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[serde(default)]
    pub connections: BTreeMap<String, ConnectionConfig>,
    #[serde(default)]
    pub targets: BTreeMap<String, TargetConfig>,
    #[serde(default)]
    pub sources: BTreeMap<String, SourceConfig>,
    /// Every connection's settings after `${ENV:NAME}` interpolation, for Python table functions.
    #[serde(skip)]
    pub connection_settings: serde_json::Map<String, serde_json::Value>,
    /// The file declaring each Python source, by source name.
    #[serde(skip)]
    pub python_modules: BTreeMap<String, String>,
}

/// How a Snowflake connection's loads get into Snowflake: required, with no default; one it cannot use fails at plan, never falling back.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoadMethod {
    /// An external stage on an S3 prefix: Snowflake reads a files source's files under it directly, and SQLLoad writes other sources' Parquet there.
    ExternalStage {
        /// The stage's location, `s3://bucket/prefix/`.
        url: String,
        /// The storage integration of the stage SQLLoad creates (`_SQLLOAD_STAGE` in each loader schema).
        #[serde(default)]
        storage_integration: Option<String>,
        /// An existing stage on `url` to use instead, `DATABASE.SCHEMA.STAGE`.
        #[serde(default)]
        name: Option<String>,
        /// The bucket's region, for SQLLoad's own writes.
        #[serde(default)]
        region: Option<String>,
        /// A custom S3 endpoint for SQLLoad's own writes (tests); Snowflake always reads the bucket itself.
        #[serde(default)]
        endpoint: Option<String>,
        /// Keep staged files after their batch commits, and leave this stage out of the cleanup of old ones.
        #[serde(default)]
        keep_staged_files: bool,
    },
    /// The user stage `@~`.
    InternalUserStage {},
    /// The table stage `@%table`: the target's for an owned direct append, otherwise the staging table's.
    InternalTableStage {},
    /// A named internal stage.
    InternalNamedStage { name: String },
    /// Snowpipe Streaming into a transient staging table.
    Streaming {},
    /// Batched multi-row `INSERT` into a transient staging table (very slow).
    Insert {},
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
        /// How loads get into Snowflake; required (`None` only so that its absence is reported with a fix).
        #[serde(default)]
        load_method: Option<Box<LoadMethod>>,
    },
    /// A Databricks SQL warehouse with Unity Catalog, reached with OAuth machine-to-machine credentials.
    Databricks {
        /// The workspace URL, such as `https://dbc-1234.cloud.databricks.com`.
        host: String,
        client_id: String,
        client_secret: crate::auth::models::Secret,
        /// The SQL warehouse statements run on.
        warehouse_id: String,
        /// The Unity Catalog catalog tables load into.
        catalog: String,
        /// The managed volume load files are staged in, `catalog.schema.volume`; it must exist.
        volume: String,
    },
    /// A BigQuery project, reached with a service-account key.
    #[serde(rename = "bigquery")]
    BigQuery {
        /// The project tables load into and jobs run in; the key's own project when `None`.
        #[serde(default)]
        project: Option<String>,
        /// Where jobs run and datasets are created, such as `EU` or `europe-west2`.
        location: String,
        /// The service-account key file's JSON text, such as `${ENV:BIGQUERY_KEY}`.
        service_account_key: crate::auth::models::Secret,
        /// Overrides the REST API's root, for tests.
        #[serde(default)]
        base_url: Option<String>,
        /// Overrides the Storage Write API's root, for tests.
        #[serde(default)]
        storage_url: Option<String>,
    },
    /// A ClickHouse server, reached over its HTTP interface.
    #[serde(rename = "clickhouse")]
    ClickHouse {
        host: String,
        /// 8123, or 8443 with `secure = true`, when omitted.
        #[serde(default)]
        port: Option<u16>,
        #[serde(default = "defaults::clickhouse_user")]
        user: String,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
        /// HTTPS instead of HTTP.
        #[serde(default)]
        secure: bool,
    },
    Postgres {
        host: String,
        #[serde(default = "defaults::postgres_port")]
        port: u16,
        user: String,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
        database: String,
        /// Whether to use TLS and what to check, as libpq's `sslmode`; `prefer` by default.
        #[serde(default)]
        sslmode: crate::postgres_source::models::SslMode,
        /// CA certificates (PEM) to trust instead of the built-in public roots.
        #[serde(default)]
        sslrootcert: Option<PathBuf>,
        /// A client certificate (PEM), with `sslkey`.
        #[serde(default)]
        sslcert: Option<PathBuf>,
        /// The client certificate's private key (PEM).
        #[serde(default)]
        sslkey: Option<PathBuf>,
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
        database: String,
    },
    /// Google Sheets, read with a service-account key: at most one of the three key settings, or the key file `GOOGLE_APPLICATION_CREDENTIALS` names.
    GoogleSheets {
        /// A path to the key file.
        #[serde(default)]
        service_account_file: Option<PathBuf>,
        /// The key file's JSON text, such as `${ENV:GOOGLE_SA_JSON}`.
        #[serde(default)]
        service_account_json: Option<crate::auth::models::Secret>,
        /// The key file's JSON text, base64-encoded.
        #[serde(default)]
        service_account_json_base64: Option<crate::auth::models::Secret>,
        /// Overrides the Sheets API's root, for tests.
        #[serde(default)]
        base_url: Option<String>,
    },
    /// An HTTP API, for Python sources; its settings reach table functions as `ctx.connection`.
    Http {
        base_url: String,
        #[serde(default)]
        token: Option<crate::auth::models::Secret>,
        /// For `sl.basic()`.
        #[serde(default)]
        username: Option<String>,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

impl ConnectionConfig {
    /// The `kind` value as written.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Snowflake { .. } => "snowflake",
            Self::Databricks { .. } => "databricks",
            Self::BigQuery { .. } => "bigquery",
            Self::ClickHouse { .. } => "clickhouse",
            Self::Postgres { .. } => "postgres",
            Self::DuckDb { .. } => "duckdb",
            Self::S3 { .. } => "s3",
            Self::Filesystem { .. } => "filesystem",
            Self::GoogleSheets { .. } => "google_sheets",
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
    /// A Python source declares a `@source.client`, which its tables share; added by discovery.
    #[serde(default)]
    pub shared_client: bool,
    /// Let the tables of a Python source that shares a client load at the same time; off because clients are rarely thread-safe.
    #[serde(default)]
    pub parallel_tables: bool,
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
    /// Typed columns (default), or each JSON object or CSV line whole in one `json` column (files sources with a text format).
    #[serde(default)]
    pub shape: RecordShape,
    /// The column `shape = "record"` loads each record into; `record` when omitted.
    #[serde(default)]
    pub record_column: Option<String>,
    /// The folder template for this source's tables in a files target, overriding the target's `path`.
    #[serde(default)]
    pub destination_path: Option<String>,
    /// Hive `key=value` folders loaded as typed columns (files sources).
    #[serde(default)]
    pub partition_columns: BTreeMap<String, String>,
    /// A template over each file's key below `path` whose `{name:type}` captures load as typed columns (files sources).
    #[serde(default)]
    pub path_columns: Option<String>,
    /// What a file whose key does not match `path_columns` does: fail `plan` (default) or be skipped.
    #[serde(default)]
    pub unmatched_paths: Option<UnmatchedPaths>,
    /// Columns with the same value in every row, typed by their TOML values (files sources).
    #[serde(default)]
    pub constant_columns: BTreeMap<String, toml::Value>,
    /// For files no external stage of the warehouse can read: download them and upload them unchanged to the user stage `@~`, where the warehouse loads them itself (much slower than an external stage; files sources).
    #[serde(default)]
    pub copy_to_internal_stage: bool,
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    #[serde(default)]
    pub skip_tables_with_columns: Vec<String>,
    #[serde(default)]
    pub allow_columns: Vec<String>,
    /// The key for `mask = { col = "hmac" }`, normally `${ENV:MASK_KEY}`.
    #[serde(default)]
    pub mask_key: Option<crate::auth::models::Secret>,
    /// Defaults for every table of the source.
    #[serde(flatten)]
    pub defaults: TableSettings,
    /// Per-table settings, keyed by `schema.table`.
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverride>,
    /// The publication streamed (`postgres_cdc`); `sqlload_<source>` when omitted, created for the selected tables if missing.
    #[serde(default)]
    pub publication: Option<String>,
    /// The replication slot holding the stream's position (`postgres_cdc`); `sqlload_<source>` when omitted.
    #[serde(default)]
    pub slot: Option<String>,
    /// The spreadsheet read (`google_sheets`), the ID in its URL.
    #[serde(default)]
    pub spreadsheet_id: Option<String>,
    /// The tables read (`google_sheets`): a tab or an A1 range by table name; every tab, named after it, when empty.
    #[serde(default)]
    pub ranges: BTreeMap<String, String>,
    /// How captured changes are written (`postgres_cdc`).
    #[serde(default)]
    pub cdc_mode: CdcMode,
    /// What a captured delete does to a merged row (`postgres_cdc` with `cdc_mode = "merge"`).
    #[serde(default)]
    pub deletes: CdcDeletes,
    /// How often `load --follow` writes the changes it has read (`postgres_cdc`); 10 seconds when omitted.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub batch_interval: Option<Duration>,
    /// The warehouse a `snowflake` source's unloads run on; the connection's when omitted.
    #[serde(default)]
    pub warehouse: Option<String>,
    /// Where a `snowflake` source unloads: an external stage, or the user stage `@~` when omitted.
    #[serde(default)]
    pub stage: Option<UnloadStage>,
}

/// Where a `snowflake` source's unloads go: an S3 URL with a storage integration, or an existing stage by name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnloadStage {
    /// The S3 folder, `s3://bucket/prefix/`, unloaded to through `storage_integration`.
    #[serde(default)]
    pub url: Option<String>,
    /// The storage integration Snowflake writes to `url` with.
    #[serde(default)]
    pub storage_integration: Option<String>,
    /// An existing stage, `DATABASE.SCHEMA.STAGE`: external on S3, or internal.
    #[serde(default)]
    pub name: Option<String>,
    /// The bucket's region, for SQLLoad's own reads of an external stage.
    #[serde(default)]
    pub region: Option<String>,
}

/// How a `postgres_cdc` source writes its changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcMode {
    /// One row per key, as the source has it now.
    #[default]
    Merge,
    /// Every change as a row of its own.
    Changelog,
}

/// What a captured delete does under `cdc_mode = "merge"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcDeletes {
    /// The row stays, with `_sqlload_deleted_at` set.
    #[default]
    Soft,
    /// The row is removed.
    Hard,
}

impl SourceConfig {
    /// Whether the source finds its tables in a database's catalog with `include` patterns (`postgres`, `postgres_cdc` or `snowflake`).
    pub fn discovers_tables(&self) -> bool {
        matches!(
            self.kind,
            SourceKind::Postgres | SourceKind::PostgresCdc | SourceKind::Snowflake
        )
    }

    /// How a files source's files are encoded, with defaults for what is not set.
    pub fn files_encoding(&self) -> FilesEncoding {
        FilesEncoding {
            format: self.format,
            compression: self.compression.unwrap_or_default(),
            csv: self.csv.clone().unwrap_or_default(),
            record: (self.shape == RecordShape::Record).then(|| {
                self.record_column
                    .clone()
                    .unwrap_or_else(|| RECORD_COLUMN.to_owned())
            }),
        }
    }

    /// The destination table name template: as set, or `{source}__{table}` for databases and Python sources and `{source}` for files.
    pub fn table_name_template(&self) -> &str {
        match (&self.table_name, self.kind) {
            (Some(template), _) => template,
            (
                None,
                SourceKind::Postgres
                | SourceKind::PostgresCdc
                | SourceKind::Snowflake
                | SourceKind::Python,
            ) => DEFAULT_TABLE_NAME,
            (None, SourceKind::Files) => DEFAULT_FILES_TABLE_NAME,
            (None, SourceKind::GoogleSheets) => DEFAULT_TABLE_NAME,
        }
    }
}

/// The declarative source kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Postgres,
    /// A Postgres database's changes, streamed from a logical replication slot.
    PostgresCdc,
    /// Existing files in an object store or directory.
    Files,
    /// Snowflake tables and queries, unloaded as Parquet and read back.
    Snowflake,
    /// Declared in Python with `sl.source` or `@sl.table`; never written in `sqlload.toml`.
    Python,
    /// Tabs or ranges of a Google Sheets spreadsheet, one table each.
    GoogleSheets,
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
    /// Read only cursor values before now plus this (a date cursor: up to and including today plus this many days).
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub lookahead: Option<Duration>,
    /// What `lookback` counts back from on incremental runs: the destination's maximum (default) or now.
    #[serde(default)]
    pub lookback_from: Option<LookbackFrom>,
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
    /// Where a Python table's position is kept: derived from the data (default) or in the run log.
    #[serde(default)]
    pub state: Option<StateMode>,
    /// Tests only, and absent from released builds: SQLLoad reads a files source's files itself, the reference side of the live parity test.
    #[cfg(feature = "container-tests")]
    #[serde(default)]
    pub test_loader_route: Option<bool>,
    /// Read and commit the cursor range one window at a time: a duration (`"1d"`) for date and timestamp cursors, an integer step for integer ones; `"none"` commits the whole read once.
    #[serde(default, deserialize_with = "deserialize_optional_batch")]
    pub batch_size: Option<BatchStep>,
    /// Checks the staged rows must pass before they are published.
    #[serde(default)]
    pub checks: Option<TableChecks>,
    /// The table engine a created table gets (ClickHouse), with its arguments.
    #[serde(default)]
    pub engine: Option<String>,
    /// The sorting key a created table gets (ClickHouse): an expression or a list of them.
    #[serde(default, deserialize_with = "deserialize_optional_keys")]
    pub order_by: Option<Vec<String>>,
    /// The partitioning expression a created table gets (ClickHouse).
    #[serde(default)]
    pub partition_by: Option<String>,
    /// The primary key a created table gets (ClickHouse), a prefix of `order_by`.
    #[serde(default, deserialize_with = "deserialize_optional_keys")]
    pub primary_key: Option<Vec<String>>,
    /// Table settings a created table gets (ClickHouse), by name.
    #[serde(default)]
    pub table_settings: Option<BTreeMap<String, LayoutSetting>>,
    /// Whether a sheet's range starts with a header row (`google_sheets`); `column_1`, `column_2`, … without one.
    #[serde(default)]
    pub header: Option<bool>,
    /// Which row of a sheet's range holds the header, counting from 1 (`google_sheets`); rows above it are skipped.
    #[serde(default)]
    pub header_row: Option<u32>,
}

/// A table setting's value in a ClickHouse layout.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum LayoutSetting {
    Bool(bool),
    Integer(i64),
    Text(String),
}

/// Checks on a table's staged rows, run before each commit publishes them; any failure fails the table and publishes nothing, unless the check is listed in `warn`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableChecks {
    /// No two staged rows share these columns' values (rows with a NULL in them are skipped, as in SQL).
    #[serde(default)]
    pub unique: Option<Vec<String>>,
    /// None of these columns holds a NULL.
    #[serde(default)]
    pub not_null: Vec<String>,
    /// The load stages at least this many rows (`replace` only).
    #[serde(default)]
    pub min_rows: Option<u64>,
    /// The table loses at most this percentage of its rows (`replace` only), written as `"20%"`.
    #[serde(default, deserialize_with = "deserialize_optional_percent")]
    pub max_row_drop: Option<u8>,
    /// The checks that only log a failure rather than failing the table.
    #[serde(default)]
    pub warn: Vec<CheckName>,
}

/// A check in `checks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckName {
    Unique,
    NotNull,
    MinRows,
    MaxRowDrop,
}

/// What a cursor's `lookback` counts back from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookbackFrom {
    /// The destination's cursor maximum.
    #[default]
    Destination,
    /// The time the run plans (a date cursor: today, UTC), for a fixed rolling window.
    Now,
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
    /// A query whose rows are the table (`snowflake` sources); the table's key is then its name.
    #[serde(default)]
    pub sql: Option<String>,
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
    /// Top-level objects of a Python table expanded one level into `<object>__<key>` columns; everything else nested stays JSON.
    #[serde(default)]
    pub flatten: Vec<String>,
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

/// A cursor table's `batch_size`: each run reads and commits one cursor window of this size at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStep {
    /// A date or timestamp cursor's window, such as `"1d"`.
    Duration(std::time::Duration),
    /// An integer cursor's window.
    Integer(u64),
    /// `"none"`: no windows and no ordered read; each read commits once.
    Whole,
}

/// Where a table's position between runs is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateMode {
    /// Derived from the data: the destination's cursor maximum.
    #[default]
    Stateless,
    /// Recorded in the append-only run log (`_sqlload_runs`), for positions the data cannot carry.
    Logged,
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
