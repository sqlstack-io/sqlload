use crate::auth::models::{Authenticator, Secret};
use crate::batching::models::{Batch, Partition};
use crate::config::models::{ColumnConfig, Config, LoadErrorPolicy, OversizedPolicy};
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::databricks_api::models::DatabricksClient;
use crate::delivery::_helpers::stage_paths::s3_target;
use crate::delivery::constants::{STAGE_CONFIRM_POLL, STREAMING_CONFIRM_POLL};
use crate::delivery::errors::DeliveryError;
use crate::http::errors::ApiError;
use crate::http::models::InFlight;
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::sql_api::models::{SqlClient, SqlRows};
use crate::stage_api::models::{DriverSession, ObjectTarget, StageInfo};
use crate::streaming_api::models::StreamingClient;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// What one delivery sent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryReport {
    pub rows: u64,
    pub uncompressed_bytes: u64,
    pub sent_bytes: u64,
    pub requests: u64,
    pub skipped_rows: u64,
    pub confirmed: BTreeMap<Partition, i64>,
}

/// Offsets Snowflake has confirmed since the last poll, newly rejected rows, and files skipped after a failed load.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Confirmation {
    pub confirmed: BTreeMap<Partition, i64>,
    pub error_rows: u64,
    pub skipped_files: u64,
    /// A retryable report or check failure from a poll that still confirmed offsets.
    pub deferred_error: Option<DeliveryError>,
}

/// The shared clients a stage delivery uses.
#[derive(Debug, Clone)]
pub struct StageClients {
    pub http: reqwest::Client,
    pub auth: Arc<Authenticator>,
    pub sql: SqlClient,
    pub driver: Arc<DriverSession>,
    pub account_url: String,
    pub in_flight: InFlight,
}

/// The destination's clients.
#[derive(Debug, Clone)]
pub enum Warehouse {
    Snowflake(SnowflakeClients),
    Databricks(DatabricksClients),
}

/// The Snowflake clients, on one authenticator.
#[derive(Debug, Clone)]
pub struct SnowflakeClients {
    pub auth: Arc<Authenticator>,
    pub sql: SqlClient,
    pub streaming: Arc<StreamingClient>,
    pub driver: Arc<DriverSession>,
    pub account_url: String,
}

/// The Databricks statement and Files API client, and the settings Zerobus Ingest streams are opened with.
#[derive(Debug, Clone)]
pub struct DatabricksClients {
    pub api: DatabricksClient,
    pub zerobus: Option<ZerobusSettings>,
}

/// Where and as whom Zerobus Ingest streams are opened.
#[derive(Debug, Clone)]
pub struct ZerobusSettings {
    pub endpoint: String,
    pub workspace_url: String,
    pub client_id: String,
    pub client_secret: Secret,
}

impl Warehouse {
    /// Run one SQL statement on the destination.
    pub async fn execute(&self, statement: &str) -> Result<SqlRows, ApiError> {
        match self {
            Self::Snowflake(snowflake) => execute_sql(&snowflake.sql, statement).await,
            Self::Databricks(databricks) => execute_statement(&databricks.api, statement).await,
        }
    }

    /// True when Snowflake signs in with a key pair, which Snowpipe's REST API needs.
    pub fn uses_key_pair(&self) -> bool {
        match self {
            Self::Snowflake(snowflake) => snowflake.auth.uses_key_pair(),
            Self::Databricks(_) => false,
        }
    }
}

/// Channel state for one stream; the committed offset token in Snowflake is the source of truth.
#[derive(Debug)]
pub struct StreamingDelivery {
    pub(crate) client: Arc<StreamingClient>,
    pub(crate) stream: Stream,
    pub(crate) max_request_bytes: u64,
    pub(crate) oversized: OversizedPolicy,
    pub(crate) continuations: BTreeMap<Partition, String>,
    pub(crate) pending: BTreeMap<Partition, i64>,
    pub(crate) error_rows: BTreeMap<String, i64>,
}

/// File uploads and loads for one stream.
#[derive(Debug)]
pub struct StageDelivery {
    pub(crate) clients: StageClients,
    pub(crate) config: Arc<Config>,
    pub(crate) stream: Stream,
    pub(crate) path: LoadPath,
    pub(crate) stage_info: Option<StageInfo>,
    pub(crate) s3: Option<ObjectTarget>,
    /// A random id for this delivery, in every staged file's name: the warehouse skips a file whose name it loaded before, so names from offsets alone could match an earlier run's file and commit offsets nothing was loaded for.
    pub(crate) run: String,
    pub(crate) staged: Option<StagedBatch>,
    pub(crate) pending: PendingLoads,
    pub(crate) begin_mark: Option<String>,
    /// Set while the stream stops: auto-ingest checks COPY_HISTORY on every poll, so the files of the last minutes are committed before it exits.
    pub(crate) final_checks: bool,
}

/// Batches handed to Snowpipe whose loads are not all reported yet, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingLoads {
    pub batches: VecDeque<PendingBatch>,
    pub policy: LoadErrorPolicy,
    pub timing: CheckTiming,
}

/// When unreported files are checked: first after `check_after`, then every `recheck_every`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckTiming {
    pub check_after: std::time::Duration,
    pub recheck_every: std::time::Duration,
}

/// One delivered batch: its files and the offsets to commit once every file is loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBatch {
    pub files: Vec<PendingFile>,
    pub offsets: BTreeMap<Partition, i64>,
}

/// One file sent to `insertFiles`, by its path relative to the pipe's location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFile {
    pub key: String,
    /// When the file was first sent, for the oldest-pending age and the history lookback.
    pub first_sent_ms: i64,
    pub resent: bool,
    /// The pipe said it had received the file before, so its load may be older than this run.
    pub received_before: bool,
    /// When the file's load history and stage object are next checked, if it is still unreported.
    pub next_check_ms: i64,
    pub last_check_ms: Option<i64>,
    /// Checks in a row that could not run, for their backoff.
    pub failed_checks: u32,
    /// A received-before file with no load history was already warned about.
    pub warned_no_history: bool,
    pub state: FileState,
}

/// Where one pending file's load stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileState {
    /// Not reported yet.
    Waiting,
    /// Reported as loading.
    InProgress,
    Loaded,
    /// Failed, with the reason; held until the policy is applied.
    Failed(String),
    /// Failed and skipped by `on_load_error = "skip"`.
    Skipped,
}

/// Pending loads settled by a poll.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settled {
    pub confirmed: BTreeMap<Partition, i64>,
    /// Files newly skipped under `on_load_error = "skip"`, with why they failed.
    pub skipped: Vec<(String, String)>,
    /// The first failure under `on_load_error = "halt"`.
    pub halted: Option<String>,
}

/// A batch's files, fixed on the first attempt so retries reuse the same paths and skip finished uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedBatch {
    pub digest: String,
    pub files: Vec<StagedFile>,
}

/// One file of a batch, relative to the internal stage or to the bucket prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    pub key: String,
    pub uploaded: bool,
}

/// Rows streamed to one Databricks table through Zerobus Ingest; a batch counts as delivered once the stream's flush returns.
#[derive(Debug)]
pub struct ZerobusDelivery {
    pub(crate) settings: ZerobusSettings,
    /// `catalog.schema.table`.
    pub(crate) table: String,
    pub(crate) columns: Vec<ColumnConfig>,
    pub(crate) max_request_bytes: u64,
    pub(crate) oversized: OversizedPolicy,
    pub(crate) link: ZerobusLink,
}

/// The open Zerobus Ingest SDK and stream, if any; dropped after a failure so the next batch opens a new stream.
#[derive(Default)]
pub struct ZerobusLink {
    pub(crate) sdk: Option<databricks_zerobus_ingest_sdk::ZerobusSdk>,
    pub(crate) stream: Option<databricks_zerobus_ingest_sdk::ZerobusStream>,
}

impl std::fmt::Debug for ZerobusLink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ZerobusLink")
            .field("open", &self.stream.is_some())
            .finish()
    }
}

/// Parquet files uploaded to a Unity Catalog volume or an S3 bucket and loaded into one Databricks table with `COPY INTO`, which skips files it loaded before.
#[derive(Debug)]
pub struct CopyDelivery {
    pub(crate) api: DatabricksClient,
    pub(crate) http: reqwest::Client,
    pub(crate) in_flight: InFlight,
    pub(crate) config: Arc<Config>,
    pub(crate) stream: Stream,
    /// The bucket, for `staging.kind = "s3"`; otherwise files go to the stream's volume.
    pub(crate) s3: Option<ObjectTarget>,
    /// A random id for this delivery, in every staged file's name: the warehouse skips a file whose name it loaded before, so names from offsets alone could match an earlier run's file and commit offsets nothing was loaded for.
    pub(crate) run: String,
    pub(crate) staged: Option<StagedBatch>,
}

/// The configured delivery mode for one stream.
#[derive(Debug)]
pub enum Delivery {
    Streaming(Box<StreamingDelivery>),
    Stage(Box<StageDelivery>),
    Zerobus(Box<ZerobusDelivery>),
    Copy(Box<CopyDelivery>),
}

impl Delivery {
    /// Snowpipe Streaming delivery for one stream.
    pub fn streaming(
        client: Arc<StreamingClient>,
        stream: Stream,
        max_request_bytes: u64,
        oversized: OversizedPolicy,
    ) -> Self {
        Self::Streaming(Box::new(StreamingDelivery::new(
            client,
            stream,
            max_request_bytes,
            oversized,
        )))
    }

    /// File delivery for one stream on `path`.
    pub fn stage(
        clients: StageClients,
        config: Arc<Config>,
        stream: Stream,
        path: LoadPath,
    ) -> Result<Self, DeliveryError> {
        StageDelivery::new(clients, config, stream, path).map(|stage| Self::Stage(Box::new(stage)))
    }

    /// Zerobus Ingest delivery for one Databricks table.
    pub fn zerobus(
        settings: ZerobusSettings,
        config: &Config,
        stream: &Stream,
        oversized: OversizedPolicy,
    ) -> Self {
        Self::Zerobus(Box::new(ZerobusDelivery {
            settings,
            table: format!(
                "{}.{}.{}",
                config.destination.database(),
                config.destination.schema(),
                stream.table
            ),
            columns: config.envelope.columns.clone(),
            max_request_bytes: config.batching.max_request_bytes.0,
            oversized,
            link: ZerobusLink::default(),
        }))
    }

    /// `COPY INTO` delivery for one Databricks table, from its volume or the `s3` bucket.
    pub fn copy(
        inputs: &DeliveryInputs,
        api: &DatabricksClient,
        stream: &Stream,
    ) -> Result<Self, DeliveryError> {
        let s3 = match inputs.path {
            LoadPath::S3Copy => Some(s3_target(&inputs.config).map_err(DeliveryError::Fatal)?),
            _ => None,
        };
        Ok(Self::Copy(Box::new(CopyDelivery {
            api: api.clone(),
            http: inputs.http.clone(),
            in_flight: inputs.in_flight.clone(),
            config: inputs.config.clone(),
            stream: stream.clone(),
            s3,
            run: uuid::Uuid::new_v4().simple().to_string(),
            staged: None,
        })))
    }

    /// True when Snowflake, not Kafka, holds the resume position.
    pub fn resumes_from_warehouse(&self) -> bool {
        matches!(self, Self::Streaming(_))
    }

    /// Prepare a partition; returns the warehouse's committed offset when the mode tracks one.
    pub async fn open(&mut self, partition: &Partition) -> Result<Option<i64>, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.open(partition).await,
            Self::Stage(_) | Self::Zerobus(_) | Self::Copy(_) => Ok(None),
        }
    }

    pub fn is_open(&self, partition: &Partition) -> bool {
        match self {
            Self::Streaming(streaming) => streaming.is_open(partition),
            Self::Stage(_) | Self::Zerobus(_) | Self::Copy(_) => true,
        }
    }

    pub async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.deliver(batch).await,
            Self::Stage(stage) => stage.deliver(batch).await,
            Self::Zerobus(zerobus) => zerobus.deliver(batch).await,
            Self::Copy(copy) => copy.deliver(batch).await,
        }
    }

    pub fn has_pending(&self) -> bool {
        match self {
            Self::Streaming(streaming) => streaming.has_pending(),
            Self::Stage(stage) => !stage.pending.is_empty(),
            Self::Zerobus(_) | Self::Copy(_) => false,
        }
    }

    /// How often [`Self::poll`] runs while confirmations are pending.
    pub fn confirm_interval(&self) -> std::time::Duration {
        match self {
            Self::Streaming(_) => STREAMING_CONFIRM_POLL,
            Self::Stage(_) | Self::Zerobus(_) | Self::Copy(_) => STAGE_CONFIRM_POLL,
        }
    }

    /// True when the stream's files sent but not committed fill `staging.max_pending_files`.
    pub fn is_full(&self) -> bool {
        match self {
            Self::Streaming(_) | Self::Zerobus(_) | Self::Copy(_) => false,
            Self::Stage(stage) => stage.is_full(),
        }
    }

    /// Files sent to Snowpipe whose offsets are not committed yet.
    pub fn pending_files(&self) -> usize {
        match self {
            Self::Streaming(_) | Self::Zerobus(_) | Self::Copy(_) => 0,
            Self::Stage(stage) => stage.pending.file_count(),
        }
    }

    /// When the oldest uncommitted file was first sent, in Unix milliseconds.
    pub fn oldest_pending_ms(&self) -> Option<i64> {
        match self {
            Self::Streaming(_) | Self::Zerobus(_) | Self::Copy(_) => None,
            Self::Stage(stage) => stage.pending.oldest_sent_ms(),
        }
    }

    pub async fn poll(&mut self) -> Result<Confirmation, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.poll().await,
            Self::Stage(stage) => stage.poll().await,
            Self::Zerobus(_) | Self::Copy(_) => Ok(Confirmation::default()),
        }
    }

    /// The stream is stopping: auto-ingest checks every waiting file on each poll from now on, instead of once per `pending_check_after`; other routes poll as before.
    pub fn check_until_stopped(&mut self) {
        if let Self::Stage(stage) = self {
            stage.final_checks = true;
        }
    }

    pub fn reset(&mut self) {
        match self {
            Self::Streaming(streaming) => streaming.reset(),
            Self::Stage(stage) => stage.pending.clear(),
            Self::Zerobus(zerobus) => zerobus.link = ZerobusLink::default(),
            Self::Copy(copy) => copy.staged = None,
        }
    }

    pub fn forget(&mut self, partitions: &[Partition]) {
        match self {
            Self::Streaming(streaming) => streaming.forget(partitions),
            Self::Stage(stage) => stage.pending.forget(partitions),
            Self::Zerobus(_) | Self::Copy(_) => {}
        }
    }

    /// Remove expired staged files; returns how many were removed.
    pub async fn prune(&self) -> Result<u64, DeliveryError> {
        match self {
            Self::Streaming(_) | Self::Zerobus(_) => Ok(0),
            Self::Stage(stage) => stage.prune().await,
            Self::Copy(copy) => copy.prune().await,
        }
    }
}

/// The clients and settings a stream's delivery needs.
#[derive(Debug, Clone)]
pub struct DeliveryInputs {
    pub http: reqwest::Client,
    pub in_flight: InFlight,
    pub warehouse: Warehouse,
    pub config: Arc<Config>,
    pub path: LoadPath,
    pub oversized: OversizedPolicy,
}
