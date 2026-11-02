use crate::auth::models::Authenticator;
use crate::batching::models::{Batch, Partition};
use crate::config::models::{Config, OversizedPolicy};
use crate::delivery::errors::DeliveryError;
use crate::naming::models::Stream;
use crate::planning::models::LoadPath;
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::{DriverSession, ObjectTarget, StageInfo};
use crate::streaming_api::models::StreamingClient;
use std::collections::BTreeMap;
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

/// Offsets Snowflake has confirmed since the last poll, and newly rejected rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Confirmation {
    pub confirmed: BTreeMap<Partition, i64>,
    pub error_rows: u64,
}

/// The shared clients a stage delivery uses.
#[derive(Debug, Clone)]
pub struct StageClients {
    pub http: reqwest::Client,
    pub auth: Arc<Authenticator>,
    pub sql: SqlClient,
    pub driver: Arc<DriverSession>,
    pub account_url: String,
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
    pub(crate) staged: Option<StagedFile>,
}

/// A batch's file, fixed on the first attempt so retries reuse the same path and skip a finished upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    pub digest: String,
    pub date: String,
    pub file: String,
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
            Self::Stage(_) => false,
        }
    }

    pub async fn poll(&mut self) -> Result<Confirmation, DeliveryError> {
        match self {
            Self::Streaming(streaming) => streaming.poll().await,
            Self::Stage(_) => Ok(Confirmation::default()),
        }
    }

    pub fn reset(&mut self) {
        if let Self::Streaming(streaming) = self {
            streaming.reset();
        }
    }

    pub fn forget(&mut self, partitions: &[Partition]) {
        if let Self::Streaming(streaming) = self {
            streaming.forget(partitions);
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
