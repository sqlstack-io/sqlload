use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::DestinationConfig;
use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::streaming_api::constants::{SCOPED_TOKEN_LIFETIME, UNAUTHORIZED};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

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
        self.last_committed_offset_token
            .as_deref()
            .and_then(|token| token.parse().ok())
    }

    /// True unless Snowflake reports the channel as invalid or closed.
    pub fn is_healthy(&self) -> bool {
        self.channel_status_code
            .as_deref()
            .is_none_or(|code| code.eq_ignore_ascii_case("SUCCESS"))
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
    pub(crate) channel_statuses: std::collections::BTreeMap<String, ChannelStatus>,
}

#[derive(Debug, Clone)]
struct IngestAccess {
    base: String,
    scoped_token: Option<String>,
    fetched: Instant,
}

/// Snowpipe Streaming client on the shared HTTP pool.
#[derive(Debug)]
pub struct StreamingClient {
    http: reqwest::Client,
    account_url: String,
    auth: Arc<Authenticator>,
    database: String,
    schema: String,
    access: Mutex<Option<IngestAccess>>,
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

    /// The ingest host for this account.
    pub async fn ingest_host(&self) -> Result<String, ApiError> {
        Ok(self.access().await?.base)
    }

    /// Open (or reopen) a named channel; reopening discards rows Snowflake has not committed.
    pub async fn open_channel(&self, pipe: &str, channel: &str) -> Result<OpenedChannel, ApiError> {
        let path = format!("/v2/streaming/{}/channels/{channel}", self.pipe_path(pipe));
        let body = json!({});
        let response = self
            .send(reqwest::Method::PUT, &path, |request| request.json(&body))
            .await?;
        let opened: OpenResponse = response.json().await?;
        Ok(OpenedChannel {
            continuation_token: opened.next_continuation_token,
            committed_offset: opened.channel_status.committed_offset(),
        })
    }

    /// Append one gzip NDJSON body whose last row has Kafka offset `offset`; returns the next continuation token.
    pub async fn append(
        &self,
        pipe: &str,
        channel: &str,
        continuation: &str,
        offset: i64,
        body: Vec<u8>,
    ) -> Result<String, ApiError> {
        let path = format!(
            "/v2/streaming/data/{}/channels/{channel}/rows",
            self.pipe_path(pipe)
        );
        let offset_token = offset.to_string();
        let response = self
            .send(reqwest::Method::POST, &path, |request| {
                request
                    .query(&[
                        ("continuationToken", continuation),
                        ("offsetToken", offset_token.as_str()),
                    ])
                    .header("Content-Type", "application/x-ndjson")
                    .header("Content-Encoding", "gzip")
                    .body(body.clone())
            })
            .await?;
        let appended: AppendResponse = response.json().await?;
        Ok(appended.next_continuation_token)
    }

    /// Status of `channels` on `pipe`, keyed by upper-case channel name.
    pub async fn channel_statuses(
        &self,
        pipe: &str,
        channels: &[String],
    ) -> Result<BTreeMap<String, ChannelStatus>, ApiError> {
        let path = format!("/v2/streaming/{}:bulk-channel-status", self.pipe_path(pipe));
        let body = json!({ "channel_names": channels });
        let response = self
            .send(reqwest::Method::POST, &path, |request| request.json(&body))
            .await?;
        let parsed: BulkStatusResponse = response.json().await?;
        Ok(parsed
            .channel_statuses
            .into_iter()
            .map(|(name, status)| (name.to_ascii_uppercase(), status))
            .collect())
    }

    fn pipe_path(&self, pipe: &str) -> String {
        format!(
            "databases/{}/schemas/{}/pipes/{pipe}",
            self.database, self.schema
        )
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        build: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, ApiError> {
        let mut refreshed = false;
        loop {
            let access = self.access().await?;
            let request = self
                .http
                .request(method.clone(), format!("{}{path}", access.base))
                .query(&[("requestId", uuid::Uuid::new_v4().to_string())]);
            let request = match &access.scoped_token {
                Some(token) => request.bearer_auth(token),
                None => {
                    let bearer = self.auth.bearer()?;
                    request
                        .bearer_auth(bearer.token)
                        .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
                }
            };
            match check_response(build(request).send().await?).await {
                Err(error) if error.status() == Some(UNAUTHORIZED) && !refreshed => {
                    refreshed = true;
                    *self.access.lock().await = None;
                }
                outcome => return outcome,
            }
        }
    }

    async fn access(&self) -> Result<IngestAccess, ApiError> {
        let mut guard = self.access.lock().await;
        if let Some(access) = guard.as_ref()
            && (access.scoped_token.is_none() || access.fetched.elapsed() < SCOPED_TOKEN_LIFETIME)
        {
            return Ok(access.clone());
        }
        let host = self.discover_host().await?;
        let scheme = if self.account_url.starts_with("http://") {
            "http"
        } else {
            "https"
        };
        let scoped_token = match self.auth.uses_key_pair() {
            true => Some(self.scoped_token(&host).await?),
            false => None,
        };
        let access = IngestAccess {
            base: format!("{scheme}://{host}"),
            scoped_token,
            fetched: Instant::now(),
        };
        *guard = Some(access.clone());
        Ok(access)
    }

    async fn discover_host(&self) -> Result<String, ApiError> {
        let bearer = self.auth.bearer()?;
        let response = self
            .http
            .get(format!("{}/v2/streaming/hostname", self.account_url))
            .bearer_auth(bearer.token)
            .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
            .send()
            .await?;
        let text = check_response(response).await?.text().await?;
        Ok(text.trim().trim_matches('"').replace('_', "-"))
    }

    async fn scoped_token(&self, host: &str) -> Result<String, ApiError> {
        let bearer = self.auth.bearer()?;
        let response = self
            .http
            .post(format!("{}/oauth/token", self.account_url))
            .bearer_auth(bearer.token)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("scope", host),
            ])
            .send()
            .await?;
        let text = check_response(response).await?.text().await?;
        let parsed: Option<serde_json::Value> = serde_json::from_str(&text).ok();
        let token = parsed
            .as_ref()
            .and_then(|value| {
                value["access_token"]
                    .as_str()
                    .or_else(|| value["token"].as_str())
            })
            .map_or_else(|| text.trim().to_owned(), str::to_owned);
        Ok(token)
    }
}
