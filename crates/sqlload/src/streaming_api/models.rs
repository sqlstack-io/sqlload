use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::DestinationConfig;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use serde::Deserialize;
use tokio::sync::Mutex;

use crate::streaming_api::constants::HEALTHY_CHANNEL;

/// A channel after opening: where to continue appending and what Snowflake has committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenedChannel {
    pub continuation_token: String,
    pub committed_offset: Option<i64>,
}

/// The status Snowflake reports for one channel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ChannelStatus {
    #[serde(default)]
    pub channel_status_code: Option<String>,
    #[serde(default)]
    pub last_committed_offset_token: Option<String>,
    #[serde(default)]
    pub rows_inserted: Option<i64>,
    #[serde(default, alias = "rows_errors")]
    pub rows_error_count: Option<i64>,
    #[serde(default)]
    pub last_error_message: Option<String>,
}

impl ChannelStatus {
    /// The committed offset token as a Kafka offset.
    pub fn committed_offset(&self) -> Option<i64> {
        let token = self.last_committed_offset_token.as_deref()?;
        match token.parse::<i64>() {
            Ok(offset) => Some(offset),
            Err(_) => None,
        }
    }

    /// True unless Snowflake reports the channel as invalid or closed.
    pub fn is_healthy(&self) -> bool {
        self.channel_status_code
            .as_deref()
            .is_none_or(|code| code.eq_ignore_ascii_case(HEALTHY_CHANNEL))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct OpenResponse {
    pub(crate) next_continuation_token: String,
    #[serde(default)]
    pub(crate) channel_status: ChannelStatus,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AppendResponse {
    pub(crate) next_continuation_token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BulkStatusResponse {
    #[serde(default)]
    pub(crate) channel_statuses: BTreeMap<String, ChannelStatus>,
}

/// Where and how to reach the ingest host.
#[derive(Debug, Clone)]
pub(crate) struct IngestAccess {
    pub(crate) base: String,
    pub(crate) scoped_token: Option<String>,
    pub(crate) fetched: Instant,
}

/// Snowpipe Streaming client on the shared HTTP pool.
#[derive(Debug)]
pub struct StreamingClient {
    pub(crate) http: reqwest::Client,
    pub(crate) account_url: String,
    pub(crate) auth: Arc<Authenticator>,
    pub(crate) database: String,
    pub(crate) schema: String,
    pub(crate) access: Mutex<Option<IngestAccess>>,
}

/// One append request: the rows of one channel after `continuation`, ending at Kafka offset `offset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest<'a> {
    pub pipe: &'a str,
    pub channel: &'a str,
    pub continuation: &'a str,
    pub offset: i64,
    pub body: Vec<u8>,
}

impl StreamingClient {
    pub fn new(
        http: reqwest::Client,
        auth: Arc<Authenticator>,
        destination: &DestinationConfig,
    ) -> Self {
        Self {
            http,
            account_url: account_url(destination),
            auth,
            database: destination.database.clone(),
            schema: destination.schema.clone(),
            access: Mutex::new(None),
        }
    }
}
