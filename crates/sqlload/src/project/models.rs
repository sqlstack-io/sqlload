use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::config::models::{AuthConfig, ByteSize};
use crate::files_source::constants::RECORD_COLUMN;
use crate::files_source::models::{
    AfterLoad, Compression, CsvOptions, FilePattern, FilesEncoding, FilesOrder, OnEmpty,
    RecordShape, SourceFormat, UnmatchedPaths, XlsxOptions, XmlOptions,
};
use crate::http_source::models::LiteralPaths;
use crate::load_naming::models::{Compat, Naming, OnDuplicateNames};
use crate::project::_helpers::defaults;
use crate::project::_helpers::serde_values::{
    deserialize_optional_auth, deserialize_optional_batch, deserialize_optional_duration,
    deserialize_optional_keys, deserialize_optional_lookback, deserialize_optional_percent,
};
use crate::project::constants::{
    DEFAULT_DESTINATION_PATH, DEFAULT_FILES_TABLE_NAME, DEFAULT_MAX_FOOTER_READS,
    DEFAULT_TABLE_NAME,
};
use crate::row_batches::models::Flatten;

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
    /// The memory a run stays under, shared between the DuckDB and MotherDuck targets and the tables loading at once; 80% of a container's or Windows job object's limit when unset, else 50% of the machine's memory; `"off"` for none. `--memory-limit` overrides it.
    #[serde(default)]
    pub memory_limit: Option<crate::memory_budget::models::MemorySetting>,
    /// Let a Postgres table read several of its `batch_size` windows at once (`batch_concurrency`), still committing them in order; off by default.
    #[serde(default)]
    pub microbatch_concurrency: bool,
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
    /// The `sl.http` paths each Python table writes as literals, by source name and table name.
    #[serde(skip)]
    pub python_http_paths: BTreeMap<String, BTreeMap<String, LiteralPaths>>,
    /// The `${ENV:NAME}` variables each connection's settings read, by connection name, for hints.
    #[serde(skip)]
    pub connection_variables: BTreeMap<String, Vec<String>>,
    /// Where settings are written in the project's file, for hints; empty for a project parsed from text alone.
    #[serde(skip)]
    pub config_lines: ConfigLines,
    /// The Python host that imported `python_paths`, which runs `transform` functions; `None` in the standalone executable.
    #[serde(skip)]
    pub python_host: Option<std::sync::Arc<dyn crate::python_sources::types::PythonHost>>,
    /// Each `transform` named in the project: its version (a hash of the file it is defined in), or why it cannot run.
    #[serde(skip)]
    pub transforms: BTreeMap<String, Result<String, String>>,
    /// Each `rename` function named in the project, or why it cannot run.
    #[serde(skip)]
    pub renames: BTreeMap<String, Result<(), String>>,
    /// The run's memory budget, worked out by the command line before the targets connect; none otherwise.
    #[serde(skip)]
    pub memory: crate::memory_budget::models::RunMemory,
}

/// The lines of a project file's sources, tables and their `unique_key` (see [`crate::project::main::config_lines::config_lines`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigLines {
    /// The file's name, as hints show it.
    pub file: String,
    /// Each line, by its setting's path, such as `["sources", "erp", "tables", "pub.items", "unique_key"]`.
    pub lines: BTreeMap<Vec<String>, usize>,
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

/// How a MotherDuck connection's rows get into MotherDuck; a stage it cannot use fails at plan, never falling back.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MotherDuckLoadMethod {
    /// zstd Parquet chunks sent from this machine, each loaded with `COPY`.
    Local {},
    /// zstd Parquet chunks written under an S3 or GCS prefix, which MotherDuck's servers read in the commit (`MD_RUN = REMOTE`).
    ExternalStage {
        /// The prefix, `s3://bucket/prefix/` or `gs://bucket/prefix/`.
        url: String,
        /// An `s3` or `gcs` connection to the bucket, whose credentials SQLLoad writes with and MotherDuck reads with; required for `gs://`, whose HMAC key MotherDuck reads with. Without it, S3 keys come from the environment, the shared credentials file or the AWS credential chain.
        #[serde(default)]
        connection: Option<String>,
        /// Keep each chunk's file after its commit.
        #[serde(default)]
        keep_staged_files: bool,
    },
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
        /// The managed volume load files are staged in, `catalog.schema.volume`; it must exist, and is needed only when a target loads into the connection.
        #[serde(default)]
        volume: Option<String>,
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
        /// A bastion to reach `host` through, over SSH.
        #[serde(default)]
        ssh: Option<crate::ssh_tunnel::models::SshTunnel>,
    },
    /// An S3 bucket; credentials come from `AWS_*` environment variables, the shared credentials file (under `profile`, else `AWS_PROFILE`), web identity, or the instance/container role.
    S3 {
        bucket: String,
        #[serde(default)]
        region: Option<String>,
        /// A custom endpoint such as MinIO.
        #[serde(default)]
        endpoint: Option<String>,
        /// The shared credentials file's profile to take keys from.
        #[serde(default)]
        profile: Option<String>,
    },
    /// A Google Cloud Storage bucket that files sources read from; credentials come from `service_account_key`, else Google's application default credentials (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud user's, or the instance's service account).
    Gcs {
        bucket: String,
        /// The service-account key file's JSON text, such as `${ENV:GCS_KEY}`.
        #[serde(default)]
        service_account_key: Option<crate::auth::models::Secret>,
        /// A custom endpoint, for tests.
        #[serde(default)]
        endpoint: Option<String>,
        /// The access ID of an HMAC key for the bucket, which DuckDB and MotherDuck need to read its Parquet files themselves (they take no service-account key).
        #[serde(default)]
        hmac_key_id: Option<String>,
        /// That HMAC key's secret, such as `${ENV:GCS_HMAC_SECRET}`.
        #[serde(default)]
        hmac_secret: Option<crate::auth::models::Secret>,
    },
    /// A directory on the machine SQLLoad runs on.
    Filesystem { root: String },
    /// An SFTP server that files sources read from; its host key must be listed in `known_hosts`.
    Sftp {
        host: String,
        /// 22 when unset.
        #[serde(default)]
        port: Option<u16>,
        user: String,
        /// The private key to sign in with (`~/` is the home folder); `password`, else ssh-agent's keys, when unset.
        #[serde(default)]
        private_key_file: Option<PathBuf>,
        #[serde(default)]
        private_key_passphrase: Option<crate::auth::models::Secret>,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
        /// The file the server's host key must be listed in; required, as SQLLoad never trusts a server on first use.
        known_hosts: PathBuf,
    },
    #[serde(rename = "duckdb")]
    DuckDb {
        /// The database file; `:memory:` for an in-memory database.
        database: String,
    },
    /// A MotherDuck database, opened through DuckDB's `md:` connection.
    #[serde(rename = "motherduck")]
    MotherDuck {
        /// The database's name, with or without `md:`; the account's default database when unset.
        #[serde(default)]
        database: Option<String>,
        /// The access token; the `motherduck_token` or `MOTHERDUCK_TOKEN` environment variable when unset.
        #[serde(default)]
        token: Option<crate::auth::models::Secret>,
        /// How rows get into MotherDuck; `local` when unset.
        #[serde(default)]
        load_method: Option<MotherDuckLoadMethod>,
    },
    /// A database reached through an ODBC driver: a connection string, or a DSN with an optional user and password.
    Odbc {
        #[serde(default)]
        connection_string: Option<crate::auth::models::Secret>,
        #[serde(default)]
        dsn: Option<String>,
        #[serde(default)]
        user: Option<String>,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
    },
    /// Google Sheets, read with a service-account key (at most one of the three key settings, or the key file `GOOGLE_APPLICATION_CREDENTIALS` names), or as a Google user who consents to an installed app's OAuth client (`client_id` and `client_secret`).
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
        /// An installed app's OAuth client, to read as the Google user who consents to it.
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<crate::auth::models::Secret>,
        /// The user's refresh token, for unattended runs; without it, the one cached at consent.
        #[serde(default)]
        refresh_token: Option<crate::auth::models::Secret>,
        /// Overrides the Sheets API's root, for tests.
        #[serde(default)]
        base_url: Option<String>,
        /// Overrides Google's OAuth token endpoint, for tests.
        #[serde(default)]
        token_url: Option<String>,
    },
    /// An HTTP API, for Python sources' `sl.http` jobs; its settings reach table functions as `ctx.connection`.
    Http {
        base_url: String,
        /// How requests authenticate, with the credentials below; `None` sends none.
        #[serde(default, deserialize_with = "deserialize_optional_auth")]
        auth: Option<crate::http_source::models::Auth>,
        #[serde(default)]
        token: Option<crate::auth::models::Secret>,
        /// For `auth` `basic` and `oauth2_password`.
        #[serde(default)]
        username: Option<String>,
        #[serde(default)]
        password: Option<crate::auth::models::Secret>,
        /// For `auth` `oauth2_client_credentials`, and `oauth2_refresh_token` or `oauth2_password` when the API wants the client too.
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<crate::auth::models::Secret>,
        /// For `auth` `oauth2_refresh_token`.
        #[serde(default)]
        refresh_token: Option<crate::auth::models::Secret>,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        /// Requests started per second, minute or hour (`10/s`), shared by every request to the connection in the run.
        #[serde(default)]
        rate_limit: Option<String>,
        /// The most requests open at once on the connection in the run.
        #[serde(default)]
        max_in_flight: Option<usize>,
        /// Retries of a request that failed with a transport error, 429 or a 5xx.
        #[serde(default)]
        retries: Option<u32>,
    },
    /// A warehouse connection the caller owns, which a library caller attaches to the project by name (see [`SessionConnection`](crate::adapters::session::models::SessionConnection)); no project file can write one.
    #[serde(skip_deserializing)]
    Session(crate::adapters::session::models::SessionConnection),
}

impl ConnectionConfig {
    /// The `kind` value as written; a caller's connection gives the kind it stands in for.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Session(session) => session.kind(),
            Self::Snowflake { .. } => "snowflake",
            Self::Databricks { .. } => "databricks",
            Self::BigQuery { .. } => "bigquery",
            Self::ClickHouse { .. } => "clickhouse",
            Self::Postgres { .. } => "postgres",
            Self::DuckDb { .. } => "duckdb",
            Self::MotherDuck { .. } => "motherduck",
            Self::S3 { .. } => "s3",
            Self::Gcs { .. } => "gcs",
            Self::Filesystem { .. } => "filesystem",
            Self::Sftp { .. } => "sftp",
            Self::Odbc { .. } => "odbc",
            Self::GoogleSheets { .. } => "google_sheets",
            Self::Http { .. } => "http",
        }
    }
}

/// Where loaded tables go in one environment: a warehouse, or files in an s3, gcs or filesystem connection.
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
    /// How CSV files are compressed (files targets with `format = "csv"`).
    #[serde(default)]
    pub compression: Option<OutputCompression>,
    /// How CSV files are written (files targets with `format = "csv"`).
    #[serde(default)]
    pub csv: Option<CsvOutput>,
    /// One file per table, or per partition, with no size split, named after the table's folder (files targets).
    #[serde(default)]
    pub single_file: Option<bool>,
    /// The most Parquet footers `plan` reads for one table's schema (files targets).
    #[serde(default)]
    pub max_footer_reads: Option<usize>,
    /// How its tables and columns are named: snake_case (default) or as the source spells them; a source or table may override it.
    #[serde(default)]
    pub naming: Option<Naming>,
    /// Whose names the columns a load adds take: SQLLoad's (default) or dlt's; a source or table may override it.
    #[serde(default)]
    pub compat: Option<Compat>,
    /// When a table's rows reach the target: all of a commit's at once (`at_commit`, the default on targets billed while they run queries) or each batch as it is read; a source or table may override it.
    #[serde(default)]
    pub send: Option<SendMode>,
    /// The folder `send = "at_commit"` holds a commit's rows in until it is sent; the system's temporary folder when unset.
    #[serde(default)]
    pub staging_dir: Option<std::path::PathBuf>,
    /// The type `json` columns get on a DuckDB or MotherDuck target: `JSON` text (default) or `VARIANT`.
    #[serde(default)]
    pub json_type: Option<JsonType>,
}

/// The type `json` columns get on a DuckDB or MotherDuck target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonType {
    /// DuckDB's `JSON`, which holds each value as its JSON text.
    #[default]
    Json,
    /// DuckDB's `VARIANT`, which keeps each value's objects, arrays, numbers and booleans typed.
    Variant,
}

/// When a table's rows reach its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendMode {
    /// Each commit's batches wait in local files, then reach the target in one burst just before the commit, so a warehouse billed while it runs queries runs none during the read.
    AtCommit,
    /// Each batch reaches the target as it is read.
    PerBatch,
}

impl TargetConfig {
    /// The table folder template: as set, or `{source}/{table}/`.
    pub fn path_template(&self) -> &str {
        self.path.as_deref().unwrap_or(DEFAULT_DESTINATION_PATH)
    }

    /// How a files target writes its files, with defaults for what is not set.
    pub fn output_encoding(&self) -> OutputEncoding {
        OutputEncoding {
            format: self.format.unwrap_or_default(),
            compression: self.compression.unwrap_or_default(),
            csv: self.csv.clone().unwrap_or_default(),
            single_file: self.single_file.unwrap_or_default(),
        }
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
    /// The schema the source's tables land in; the target's `loader_schema` when omitted. The source schema is chosen by `include`.
    #[serde(default)]
    pub destination_schema: Option<String>,
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
    /// Which worksheet rows load (files sources with `format = "xlsx"`).
    #[serde(default)]
    pub xlsx: Option<XlsxOptions>,
    /// Which elements are rows (files sources with `format = "xml"`).
    #[serde(default)]
    pub xml: Option<XmlOptions>,
    /// With `compression = "zip"`, a pattern the members' names must match (files sources).
    #[serde(default)]
    pub members: Option<String>,
    /// A glob each file's path below `path` must match, such as `"file_*.txt"`, in place of the format's name suffix (files sources).
    #[serde(default)]
    pub pattern: Option<String>,
    /// What to do with each file once its rows commit (files sources).
    #[serde(default)]
    pub after_load: AfterLoad,
    /// The order a merge applies files in: oldest modified first (default) or by path (files sources).
    #[serde(default)]
    pub order: FilesOrder,
    /// What a load does when the folder exists and holds no files: fail (default) or skip the table (files sources).
    #[serde(default)]
    pub on_empty: OnEmpty,
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
    /// How often `load --follow` writes the changes it has read (`postgres_cdc`); 10 seconds when omitted.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub batch_interval: Option<Duration>,
    /// The warehouse a `snowflake` source's unloads run on; the connection's when omitted.
    #[serde(default)]
    pub warehouse: Option<String>,
    /// Where a `snowflake` source unloads: an external stage, or the user stage `@~` when omitted.
    #[serde(default)]
    pub stage: Option<UnloadStage>,
    /// The largest text value an `odbc` source's reads hold, in bytes; unbounded columns (`TEXT`, `VARCHAR(MAX)`) need it.
    #[serde(default)]
    pub max_text_size: Option<usize>,
    /// The largest binary value an `odbc` source's reads hold, in bytes.
    #[serde(default)]
    pub max_binary_size: Option<usize>,
    /// The size over which an `odbc` source's text or binary column is read value by value rather than into a buffer per row; 4 KiB when omitted.
    #[serde(default)]
    pub long_column_threshold: Option<ByteSize>,
    /// The memory one fetch of an `odbc` source's table holds; worked out from the machine's memory and `concurrency` when omitted.
    #[serde(default)]
    pub max_batch_memory: Option<ByteSize>,
    /// The project a `bigquery` source's tables live in; the connection's when omitted.
    #[serde(default)]
    pub project: Option<String>,
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

impl SourceConfig {
    /// The `constant_columns` of table `table` (its key under `tables`): its own map, else the source's; a table's map replaces the source's.
    pub fn table_constants(&self, table: &str) -> BTreeMap<String, toml::Value> {
        self.tables
            .get(table)
            .and_then(|overrides| overrides.settings.constant_columns.as_ref())
            .or(self.defaults.constant_columns.as_ref())
            .map(|constants| constants.0.clone())
            .unwrap_or_default()
    }

    /// Whether the source finds its tables in a database's catalog with `include` patterns (every database source kind).
    pub fn discovers_tables(&self) -> bool {
        matches!(
            self.kind,
            SourceKind::Postgres
                | SourceKind::PostgresCdc
                | SourceKind::Snowflake
                | SourceKind::Odbc
                | SourceKind::BigQuery
                | SourceKind::Databricks
                | SourceKind::DuckDb
                | SourceKind::MotherDuck
        )
    }

    /// How a files source's files are encoded, with defaults for what is not set; the elements a table's `unnest` paths end in are XML `lists`, so one that appears once still lands in its child table.
    pub fn files_encoding(&self) -> FilesEncoding {
        let mut xml = self.xml.clone().unwrap_or_default();
        for path in self.tables.values().flat_map(|table| match &table.unnest {
            Some(PathsSetting::Paths(paths)) => paths.as_slice(),
            Some(PathsSetting::Word(_)) | None => &[],
        }) {
            let element = path.rsplit('.').next().unwrap_or_default().to_owned();
            if !xml.lists.contains(&element) {
                xml.lists.push(element);
            }
        }
        let column_names = self
            .tables
            .values()
            .find_map(|table| table.settings.column_names.clone())
            .or_else(|| self.defaults.column_names.clone());
        FilesEncoding {
            format: self.format,
            compression: self.compression.unwrap_or_default(),
            csv: CsvOptions {
                column_names: column_names.clone(),
                ..self.csv.clone().unwrap_or_default()
            },
            xlsx: XlsxOptions {
                column_names,
                ..self.xlsx.clone().unwrap_or_default()
            },
            xml,
            members: self.members.clone(),
            pattern: match self.pattern.as_deref().map(FilePattern::new) {
                Some(Ok(pattern)) => Some(pattern),
                Some(Err(_)) | None => None,
            },
            record: (self.shape == RecordShape::Record).then(|| {
                self.record_column
                    .clone()
                    .unwrap_or_else(|| RECORD_COLUMN.to_owned())
            }),
            flatten: Flatten::default(),
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
                | SourceKind::Odbc
                | SourceKind::BigQuery
                | SourceKind::Databricks
                | SourceKind::DuckDb
                | SourceKind::MotherDuck
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
    /// Tables and queries of a database with an ODBC driver, read into Arrow.
    Odbc,
    /// BigQuery tables, views and queries, read as Arrow through the Storage Read API.
    #[serde(rename = "bigquery")]
    BigQuery,
    /// Databricks tables, views and queries, read as Arrow through the Statement Execution API's result links.
    Databricks,
    /// Tables, views and queries of a DuckDB database file, read as Arrow from a read-only connection.
    #[serde(rename = "duckdb")]
    DuckDb,
    /// Tables, views and queries of a MotherDuck database, read as Arrow through the MotherDuck bridge.
    #[serde(rename = "motherduck")]
    MotherDuck,
    /// Declared in Python with `sl.source` or `@sl.table`; never written in `sqlload.toml`.
    Python,
    /// Tabs or ranges of a Google Sheets spreadsheet, one table each.
    GoogleSheets,
}

impl SourceKind {
    /// The kind as `kind` spells it in `sqlload.toml`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::PostgresCdc => "postgres_cdc",
            Self::Files => "files",
            Self::Snowflake => "snowflake",
            Self::Odbc => "odbc",
            Self::BigQuery => "bigquery",
            Self::Databricks => "databricks",
            Self::DuckDb => "duckdb",
            Self::MotherDuck => "motherduck",
            Self::Python => "python",
            Self::GoogleSheets => "google_sheets",
        }
    }
}

/// The file formats a files target writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesFormat {
    #[default]
    Parquet,
    Csv,
}

/// How a files target compresses CSV files; Parquet compresses inside the file (zstd).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputCompression {
    #[default]
    None,
    Gzip,
    Zstd,
}

/// How a files target writes CSV files.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CsvOutput {
    /// The field separator, one ASCII character.
    pub delimiter: char,
    /// Whether the first line holds the column names.
    pub header: bool,
    /// The quote character, one ASCII character; a quote inside a quoted field is written twice.
    pub quote: char,
    /// What a NULL is written as, unquoted; a text value equal to it is quoted, so the two stay apart.
    pub null_value: String,
}

impl Default for CsvOutput {
    fn default() -> Self {
        Self {
            delimiter: ',',
            header: true,
            quote: '"',
            null_value: String::new(),
        }
    }
}

/// How a files target writes its files.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OutputEncoding {
    pub format: FilesFormat,
    pub compression: OutputCompression,
    pub csv: CsvOutput,
    pub single_file: bool,
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
    /// The cursor column, or a list of them read as one cursor: the first non-NULL value of each row (`COALESCE`).
    #[serde(default, deserialize_with = "deserialize_optional_keys")]
    pub cursor_column: Option<Vec<String>>,
    #[serde(default)]
    pub cursor_start: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_lookback")]
    pub lookback: Option<Lookback>,
    /// Read only cursor values before now plus this (a date cursor: up to and including today plus this many days).
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub lookahead: Option<Duration>,
    /// What `lookback` counts back from on incremental runs: the destination's maximum (default) or now.
    #[serde(default)]
    pub lookback_from: Option<LookbackFrom>,
    #[serde(default)]
    pub append_cursor_inclusive: Option<bool>,
    /// How an ODBC source writes the cursor's bounds into its query: bound parameters (default) or ODBC escape literals.
    #[serde(default)]
    pub cursor_bounds: Option<CursorBounds>,
    /// What an incremental read does with source rows whose cursor is NULL: fail the load (default), read them on every run (`merge` only), or skip them.
    #[serde(default)]
    pub null_cursor: Option<NullCursor>,
    /// What a Python table's load does with the rows its function returns outside the cursor window it was asked for: fail the table (default), skip them, or load them with a warning.
    #[serde(default)]
    pub on_cursor_out_of_range: Option<OnCursorOutOfRange>,
    #[serde(default)]
    pub contract: Option<Contract>,
    #[serde(default)]
    pub on_schema_change: Option<OnSchemaChange>,
    #[serde(default)]
    pub auto_widen: Option<bool>,
    /// `infer_types = false`: every column whose type SQLLoad would infer from sampled values loads as `text` (objects and lists stay `json`); pinned columns keep their types.
    #[serde(default)]
    pub infer_types: Option<bool>,
    /// What the dict rows of a Python table or transform do with a NaN or infinite float: keep it where the column holds it and fail where it can't (default), or load it as NULL.
    #[serde(default)]
    pub on_nonfinite: Option<OnNonFinite>,
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
    /// The most `batch_size` windows of a Postgres table read at once, committed in order; needs `microbatch_concurrency = true`.
    #[serde(default)]
    pub batch_concurrency: Option<std::num::NonZeroUsize>,
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
    /// The columns' names, in order, for data without a header row (CSV and Excel files, `google_sheets`); the count must match the data's columns.
    #[serde(default)]
    pub column_names: Option<Vec<String>>,
    /// Columns with the same value in every row, typed by their TOML values; a table's map replaces its source's.
    #[serde(default)]
    pub constant_columns: Option<ConstantColumns>,
    /// What a merge does with rows of one commit that share a `unique_key`: keep one (default) or fail.
    #[serde(default)]
    pub on_duplicate_keys: Option<OnDuplicateKeys>,
    /// What a merge does with a NULL in a `unique_key` column, and `delete_insert` in a `replace_key` column: match it with a NULL (default) or fail the commit.
    #[serde(default)]
    pub null_keys: Option<NullKeys>,
    /// The columns whose values `delete_insert` replaces whole: each commit first deletes the rows holding a value of them that its rows hold.
    #[serde(default, deserialize_with = "deserialize_optional_keys")]
    pub replace_key: Option<Vec<String>>,
    /// What a files source's `delete_insert` by `replace_key` does when two files of its folder hold one key value: fail (default) or let the file last in `order` replace it.
    #[serde(default)]
    pub on_overlapping_files: Option<OnOverlappingFiles>,
    /// The column, or columns compared in order, whose higher value is the newer version of a row: of the rows sharing a `unique_key` a merge keeps the newest, and never overwrites a loaded row with an older one.
    #[serde(default, deserialize_with = "deserialize_optional_keys")]
    pub newest_by: Option<Vec<String>>,
    /// How the table and its columns are named: snake_case (default) or as the source spells them; overrides the target's.
    #[serde(default)]
    pub naming: Option<Naming>,
    /// Whose names the columns a load adds take: SQLLoad's (default) or dlt's, with a row id on every row; overrides the target's.
    #[serde(default)]
    pub compat: Option<Compat>,
    /// When the table's rows reach the target: at each commit, or each batch as it is read; overrides the target's.
    #[serde(default)]
    pub send: Option<SendMode>,
    /// Names for columns the naming leaves unfit: a map from source name to loaded name, or the name `@sl.rename` registered a Python function under.
    #[serde(default)]
    pub rename: Option<RenameSetting>,
    /// What the load does with columns that would load under one name: number them (default) or fail.
    #[serde(default)]
    pub on_duplicate_names: Option<OnDuplicateNames>,
    /// The IANA time zone, such as `Europe/Paris`, of the times without an offset that `parse` reads in a column without its own `tz` (UTC when unset).
    #[serde(default)]
    pub timezone: Option<String>,
    /// A condition in the source database's SQL (`where`), ANDed into every read of a table SQLLoad selects from, next to its cursor filter; a table's own replaces its source's.
    #[serde(default, rename = "where")]
    pub row_filter: Option<String>,
    /// Give every row a unique id column (`_sqlload_row_id`); always on under `compat = "dlt"`, as `_dlt_id`.
    #[serde(default)]
    pub row_id: Option<bool>,
    /// What a merge does with a row whose key the source no longer holds (`soft` sets `_sqlload_deleted_at`, `hard` removes it; unset, rows are never marked), and a captured delete under `cdc_mode = "merge"` (soft when unset).
    #[serde(default)]
    pub deletes: Option<Deletes>,
    /// The name `@sl.transform` registered a Python function under, found on `python_paths`, run on each file's rows (files) or each range's (`google_sheets`) before they load.
    #[serde(default)]
    pub transform: Option<String>,
}

/// `rename`: loaded names by source name, or a Python function's registered name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum RenameSetting {
    Map(BTreeMap<String, String>),
    Function(String),
}

/// What a merge does with rows of one commit that share a `unique_key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDuplicateKeys {
    /// Keep one row per key: the last in source order, or the newest by `newest_by`.
    #[default]
    Dedupe,
    /// Fail the commit, naming some of the repeated keys.
    Fail,
}

/// What a files source's `delete_insert` by `replace_key` does when two files of its folder hold one key value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnOverlappingFiles {
    /// Fail the file's commit, naming both files: they may be parts of one export.
    #[default]
    Fail,
    /// The file's commit replaces the value's rows, whichever file loaded them: the file last in `order` wins.
    LastWins,
}

/// What a merge does with a row whose key the source no longer holds, found by `load --sync-deletes`, or a captured delete does to a merged row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Deletes {
    /// The row stays, with `_sqlload_deleted_at` set; a merge clears it when the key comes back.
    Soft,
    /// The row is removed.
    Hard,
}

/// What a merge does with a NULL in a `unique_key` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullKeys {
    /// Match it with a NULL, as a value like any other: a row whose key holds one updates the row holding the same key.
    #[default]
    Match,
    /// Fail the commit holding it, naming the key's columns and some of the keys.
    Fail,
}

/// `constant_columns`: each column's value as written, by name.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(transparent)]
pub struct ConstantColumns(pub BTreeMap<String, toml::Value>);

/// For the settings that derive `Eq`; a `nan` constant is never equal to itself, which only makes two settings compare unequal.
impl Eq for ConstantColumns {}

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

/// How an ODBC source writes a cursor's bounds into its query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorBounds {
    /// `?` placeholders bound as the cursor's type.
    #[default]
    Parameters,
    /// ODBC escapes written into the query: `{ts '…'}`, `{d '…'}`, numbers and quoted text, for a driver that refuses a bound value.
    Literals,
}

/// What an incremental read does with source rows whose cursor is NULL, which a cursor filter never selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullCursor {
    /// Count them before the read and fail the table when there are any.
    #[default]
    Fail,
    /// Read them on every run beside the cursor's range (`merge` only).
    Include,
    /// Skip them, as a cursor filter does.
    Ignore,
}

/// What a Python table's load does with the rows its function returns outside the cursor window it was asked for (a NULL cursor is `null_cursor`'s).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnCursorOutOfRange {
    /// The window does not commit, and the table fails naming the rows and their likely cause.
    #[default]
    Fail,
    /// Drop them: a later window reads the rows past its end, and an earlier one read those before its start.
    Skip,
    /// Load them, with a warning in the load report.
    Warn,
}

/// What the dict rows of a Python table or transform do with NaN, `inf` and `-inf` floats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnNonFinite {
    /// Load them as floats, and fail naming the column where its type can't hold them (a decimal or integer, a `json` value).
    #[default]
    Keep,
    /// Load them as NULL.
    Null,
}

/// How far a cursor's `lookback` reads back: a duration on a date or timestamp cursor (`"1d"`), or a number of cursor units on an integer cursor (`1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookback {
    Duration(Duration),
    Units(i64),
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
    /// Objects whose keys load as `<object>__<key>` columns: dotted paths (`"customer"`, `"customer.address"`, `"items.dimensions"` inside unnested items), or `"all"`; everything else nested stays JSON.
    #[serde(default)]
    pub flatten: Option<PathsSetting>,
    /// List fields whose items become rows of child tables: dotted paths (`"items"`, `"items.parts"`), or `"all"`.
    #[serde(default)]
    pub unnest: Option<PathsSetting>,
    /// How many levels down `unnest` makes child tables and `flatten` flattens objects at most; deeper lists and objects stay JSON (default 1000, as dlt's).
    #[serde(default)]
    pub max_depth: Option<u32>,
    /// Text columns read as times, by column name: `"epoch_ms"`, or `{ timestamp = ["iso", "%m/%d/%Y %H:%M"], tz = "Europe/Paris", on_error = "null" }` (see [`column_parse`](crate::column_parse::main::column_parse::column_parse)).
    #[serde(default)]
    pub parse: BTreeMap<String, serde_json::Value>,
    /// Where a source records the keys it deleted (`deletes_from = { table, cursor_column, lookback }`), which mark the table's rows under `deletes`.
    #[serde(default)]
    pub deletes_from: Option<DeletesFrom>,
    /// The key under `tables` of the table this one holds the tombstones of: set on the table SQLLoad adds for a `deletes_from`, never read from the project.
    #[serde(skip)]
    pub tombstones_of: Option<String>,
}

/// A source table recording deleted keys: each row a deleted row's `unique_key` values and when it was deleted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletesFrom {
    /// The tombstone table, as `schema.table`.
    pub table: String,
    /// Its column holding when each key was deleted, read incrementally as a cursor.
    pub cursor_column: String,
    /// How far before the latest tombstone loaded each load reads again, for tombstones that become visible after a later one loaded (written by a long transaction); the window read again replaces its rows, so none repeat.
    #[serde(default, deserialize_with = "deserialize_optional_lookback")]
    pub lookback: Option<Lookback>,
}

/// A table's `unnest` or `flatten` as written: a word (`"all"`) or a list of dotted paths.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum PathsSetting {
    Word(String),
    Paths(Vec<String>),
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

/// A MotherDuck database setting, or a DuckDB path, split into its parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MotherDuckPath<'a> {
    /// It starts with `md:` or `motherduck:`, in any case.
    pub prefixed: bool,
    /// The bare database name; `None` for the account's default database.
    pub name: Option<&'a str>,
    /// What follows `?`, if anything.
    pub options: &'a str,
}
