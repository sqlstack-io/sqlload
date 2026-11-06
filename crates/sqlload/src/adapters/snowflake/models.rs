use crate::adapters::contract::models::TableName;
use crate::auth::models::Authenticator;
use crate::column_types::models::ColumnType;
use crate::config::models::{AuthConfig, DestinationConfig};
use crate::file_store::models::OpenFolder;
use crate::files_source::models::{DataFile, FilesEncoding, SourceFormat, StoredRead};
use crate::loader::errors::LoaderError;
use crate::parquet_parts::models::PartWriter;
use crate::project::models::MaskMethod;
use crate::schema_plan::models::DesiredColumn;
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;
use crate::stage_api::models::StageInfo;
use crate::store_paths::models::Location;
use crate::streaming_api::models::StreamingClient;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// An authenticated Snowflake account, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    pub sql: SqlClient,
    pub driver: Arc<DriverSession>,
    pub http: reqwest::Client,
    pub database: String,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
    /// The route the connection's `load_method` sets.
    pub stage: ForcedStage,
    /// What this run's plan already found out about stages, by what was probed: usable (and whether `apply` must create it), or why not.
    pub stage_checks: Arc<Mutex<BTreeMap<String, Result<bool, StepFailure>>>>,
    /// What a streaming client for this account needs.
    pub auth: Arc<Authenticator>,
    pub config: DestinationConfig,
}

/// The route a connection's `load_method` sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForcedStage {
    User,
    Table,
    Streaming,
    Insert,
    Internal { name: String },
    External(ExternalStage),
}

/// An S3 external stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalStage {
    /// The stage URL as a folder, with the region and endpoint SQLLoad writes through.
    pub location: Location,
    pub object: StageObject,
    /// The target's loader schema (`table` empty), where SQLLoad maintains its stage whatever schema a table loads into.
    pub home: TableName,
    /// Keep staged files after their batch commits, and leave the stage out of the cleanup of old ones.
    pub keep_staged_files: bool,
}

/// Which Snowflake stage object reads the URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageObject {
    /// `_SQLLOAD_STAGE` in each loader schema, created with this storage integration.
    Managed { integration: String },
    /// An existing stage, `DATABASE.SCHEMA.STAGE`.
    Existing { name: String },
}

/// How one table's rows reach Snowflake, as its plan chose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageRoute {
    External(ExternalStage),
    Internal {
        name: String,
    },
    /// `@~`.
    User,
    /// The target's `@%table`: an owned direct append.
    TargetTable,
    /// The staging table's `@%table`, which the load always owns.
    StagingTable,
    /// Snowpipe Streaming into the staging table.
    Streaming,
    /// Batched multi-row `INSERT` into the staging table.
    Insert,
}

/// One connection's settings.
#[derive(Debug, Clone)]
pub struct SnowflakeAccount {
    pub account: String,
    pub user: String,
    pub auth: AuthConfig,
    pub role: Option<String>,
    pub warehouse: Option<String>,
    pub database: String,
    pub base_url: Option<String>,
    pub stage: ForcedStage,
}

/// One batch's staged files and where they load.
#[derive(Debug, Clone, Copy)]
pub struct CopyStaged<'a> {
    pub table: &'a TableName,
    /// The stage as SQL, `@~` or `@"DB"."SCHEMA"."STAGE"`.
    pub stage: &'a str,
    /// Below the stage, ending in `/`.
    pub folder: &'a str,
    pub names: &'a [String],
    /// The columns to fill by a `SELECT`, for a table holding JSON the files carry as text (`PARSE_JSON`); `None` loads by column name.
    pub selected: Option<&'a [DesiredColumn]>,
}

/// Where one staged file goes on an internal stage.
#[derive(Debug, Clone, Copy)]
pub struct StagedFile<'a> {
    /// The stage without its `@`, such as `~` or `"DB"."SCHEMA".%"TABLE"`.
    pub stage: &'a str,
    pub key: &'a str,
    pub body: &'a [u8],
}

/// Snowflake's SQL dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct SnowflakeSql;

/// Where one column of a direct `COPY` gets its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyValue {
    /// The file's column of the same name; `file_type` is the type the files hold it as (`None` when no sampled file has it), `stored` how a stage read returns it, and `short` that the live column keeps fewer than 9 digits after the second, so the value is cut to microseconds toward zero as SQLLoad's own reader does.
    File {
        file_type: Option<ColumnType>,
        stored: StoredRead,
        short: bool,
    },
    /// The file's value of this partition column, from its path.
    Partition,
    /// The file's full name, as the files source records it.
    SourceFile,
    /// The file's version (ETag, or size and modification time), as the files source records it.
    SourceVersion,
    /// The row's number in its file, from 1.
    SourceRow,
    /// A SQL expression, the same for every row (load metadata).
    Literal(String),
}

/// One column a direct `COPY` fills.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyColumn {
    pub name: String,
    pub column_type: ColumnType,
    pub value: CopyValue,
}

/// Everything one table's direct `COPY` statements need.
#[derive(Debug, Clone, Copy)]
pub struct DirectCopy<'a> {
    /// The table copied into: the target itself (`append`) or the load's staging table.
    pub target: &'a TableName,
    /// The stage, as SQL (`@"DB"."SCHEMA"."_SQLLOAD_STAGE"` or `@~`).
    pub stage: &'a str,
    /// The files' folder below the stage, which `FILES` names them in.
    pub folder: &'a str,
    /// What `METADATA$FILENAME` starts each file's name with: the folder's bucket-relative key on an external stage, `@~/` and the folder on the user stage.
    pub filename_prefix: &'a str,
    /// The files source's folder.
    pub source: &'a Location,
    pub files: &'a [DataFile],
    pub columns: &'a [CopyColumn],
    pub encoding: &'a FilesEncoding,
    /// Columns whose types were inferred from sampled values, whose every value must fit the inferred type (JSON, NDJSON and CSV).
    pub inferred: &'a [String],
    /// CSV files' column names by relative key, which place each column.
    pub headers: &'a std::collections::BTreeMap<String, Vec<String>>,
}

/// One `COPY` statement and the files it loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBatch {
    pub sql: String,
    /// The files' keys below the source folder.
    pub keys: Vec<String>,
}

/// What decides whether Snowflake can load a files source's files itself, exactly as SQLLoad would.
#[derive(Debug, Clone, Copy)]
pub struct DirectCheck<'a> {
    /// The external stage's URL the files are read under; `None` for files copied unchanged to an internal stage.
    pub stage_url: Option<&'a Location>,
    pub source: &'a Location,
    pub format: SourceFormat,
    pub files: &'a [DataFile],
    pub columns: &'a [CopyColumn],
    /// Columns the files hold in a way a direct `COPY` does not load exactly, and how.
    pub refused: &'a [(String, String)],
    /// Masked columns and how.
    pub masks: &'a [(String, MaskMethod)],
}

/// How SQLLoad finds a stage object, and what it must point at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageLookup {
    /// `SHOW STAGES LIKE ... IN SCHEMA ...`.
    pub query: String,
    /// The stage's name as `SHOW STAGES` reports it.
    pub name: String,
}

/// A stage as `SHOW STAGES` reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShownStage {
    pub name: String,
    pub url: String,
    pub storage_integration: String,
}

/// A stage `apply` creates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStage {
    /// As SQL, which also tells stages apart.
    pub name: String,
    pub url: String,
    pub statements: Vec<String>,
}

/// What decides a table's route.
#[derive(Debug, Clone, Copy)]
pub struct RouteRequest<'a> {
    pub destination: &'a TableName,
    pub table_exists: bool,
    /// The load would `COPY` straight into the target (`append`, not logged).
    pub direct_append: bool,
    pub snowflake: &'a SnowflakeDestination,
}

/// Why a step of the chain cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepFailure {
    /// The role may not use it, it does not exist, or the account's stages are not on S3: the chain moves on.
    Unusable(String),
    /// Anything else, such as a network failure, which says nothing about the step: the plan fails, so a blip cannot change a table's route.
    Failed(String),
}

/// A channel a load's rows stream through.
#[derive(Debug)]
pub struct RowStream {
    pub client: StreamingClient,
    pub pipe: String,
    pub channel: String,
    pub continuation: String,
    /// The offset of the last request sent, 0 before any.
    pub offset: i64,
}

/// One table's write session on Snowflake: Parquet files staged and loaded by `COPY`, or rows sent without files.
#[derive(Debug)]
pub struct SnowflakeWriter {
    pub destination: SnowflakeDestination,
    /// How this load's rows go in.
    pub route: Option<StageRoute>,
    /// The folder of an external stage, written with SQLLoad's own client.
    pub external_folder: Option<OpenFolder>,
    /// The channel rows are streaming through.
    pub stream: Option<RowStream>,
    /// The batch's Parquet being written, and the names of its files staged so far.
    pub parts: PartWriter,
    pub staged: Vec<String>,
    /// The upload credentials this batch fetched, for all its files.
    pub stage: Option<StageInfo>,
    /// The batch's files still uploading, a few at a time, while the next ones are read and encoded.
    pub uploads: tokio::task::JoinSet<Result<(), LoaderError>>,
}

/// Where a direct batch's `COPY` reads its files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyPlace {
    /// The stage, as SQL.
    pub stage: String,
    /// The files' folder below the stage.
    pub folder: String,
    /// What `METADATA$FILENAME` starts each file's name with.
    pub filename_prefix: String,
    /// The files were uploaded to an internal stage for this batch, and are removed after it.
    pub uploaded: bool,
}

/// A batch's statements through a temporary table, which hashes masked columns: sent in one request, or split over several through a delta table whose masked columns are already hashed.
#[derive(Debug)]
pub(in crate::adapters) struct HashedScript {
    /// Creates the temporary table the `COPY`s fill with plain values.
    pub(in crate::adapters) create: String,
    pub(in crate::adapters) copies: Vec<String>,
    /// Hashes the temporary table into the target, in one request.
    pub(in crate::adapters) write: Vec<String>,
    pub(in crate::adapters) drop_staging: String,
    /// Creates the transient delta table a split batch hashes into.
    pub(in crate::adapters) create_delta: String,
    /// Moves the temporary table into the delta, hashing masked columns; nothing is cast.
    pub(in crate::adapters) hash_into_delta: Vec<String>,
    /// Writes the delta into the target.
    pub(in crate::adapters) finish: Vec<String>,
    /// Dropped after the split batch, whether or not it loaded; a failed drop only warns.
    pub(in crate::adapters) drop_delta: String,
}
