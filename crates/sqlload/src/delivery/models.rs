use crate::auth::models::Authenticator;
use crate::batching::models::{Batch, Partition};
use crate::config::models::{Config, LoadErrorPolicy, OversizedPolicy};
use crate::delivery::constants::{STAGE_CONFIRM_POLL, STREAMING_CONFIRM_POLL};
use crate::delivery::errors::DeliveryError;
use crate::http::models::InFlight;
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;
use crate::sql_api::models::SqlClient;
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
    pub(crate) staged: Option<StagedBatch>,
    pub(crate) pending: PendingLoads,
    pub(crate) begin_mark: Option<String>,
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

/// The configured delivery mode for one stream.
#[derive(Debug)]
pub enum Delivery {
    Streaming(Box<StreamingDelivery>),
    Stage(Box<StageDelivery>),
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

    /// True when Snowflake, not Kafka, holds the resume position.
    pub fn resumes_from_warehouse(&self) -> bool {
        matches!(self, Self::Streaming(_))
    }

    /// Prepare a partition; returns the warehouse's committed offset when the mode tracks one.
    pub async fn open(&mut self, partition: &Partition) -> Result<Option<i64>, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.open(partition).await,
            Self::Stage(_) => Ok(None),
        }
    }

    pub fn is_open(&self, partition: &Partition) -> bool {
        match self {
            Self::Streaming(streaming) => streaming.is_open(partition),
            Self::Stage(_) => true,
        }
    }

    pub async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.deliver(batch).await,
            Self::Stage(stage) => stage.deliver(batch).await,
        }
    }

    pub fn has_pending(&self) -> bool {
        match self {
            Self::Streaming(streaming) => streaming.has_pending(),
            Self::Stage(stage) => !stage.pending.is_empty(),
        }
    }

    /// How often [`Self::poll`] runs while confirmations are pending.
    pub fn confirm_interval(&self) -> std::time::Duration {
        match self {
            Self::Streaming(_) => STREAMING_CONFIRM_POLL,
            Self::Stage(_) => STAGE_CONFIRM_POLL,
        }
    }

    /// True when the stream's files sent but not committed fill `staging.max_pending_files`.
    pub fn is_full(&self) -> bool {
        match self {
            Self::Streaming(_) => false,
            Self::Stage(stage) => stage.is_full(),
        }
    }

    /// Files sent to Snowpipe whose offsets are not committed yet.
    pub fn pending_files(&self) -> usize {
        match self {
            Self::Streaming(_) => 0,
            Self::Stage(stage) => stage.pending.file_count(),
        }
    }

    /// When the oldest uncommitted file was first sent, in Unix milliseconds.
    pub fn oldest_pending_ms(&self) -> Option<i64> {
        match self {
            Self::Streaming(_) => None,
            Self::Stage(stage) => stage.pending.oldest_sent_ms(),
        }
    }

    pub async fn poll(&mut self) -> Result<Confirmation, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.poll().await,
            Self::Stage(stage) => stage.poll().await,
        }
    }

    pub fn reset(&mut self) {
        match self {
            Self::Streaming(streaming) => streaming.reset(),
            Self::Stage(stage) => stage.pending.clear(),
        }
    }

    pub fn forget(&mut self, partitions: &[Partition]) {
        match self {
            Self::Streaming(streaming) => streaming.forget(partitions),
            Self::Stage(stage) => stage.pending.forget(partitions),
        }
    }

    /// Remove expired staged files; returns how many were removed.
    pub async fn prune(&self) -> Result<u64, DeliveryError> {
        match self {
            Self::Streaming(_) => Ok(0),
            Self::Stage(stage) => stage.prune().await,
        }
    }
}

/// The clients and settings a stream's delivery needs.
#[derive(Debug, Clone)]
pub struct DeliveryInputs {
    pub streaming: Arc<StreamingClient>,
    pub stage: StageClients,
    pub config: Arc<Config>,
    pub path: LoadPath,
    pub oversized: OversizedPolicy,
}
