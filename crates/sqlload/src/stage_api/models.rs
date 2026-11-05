use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::auth::models::Secret;
use crate::config::models::DestinationConfig;
use crate::http::errors::ApiError;
use crate::stage_api::_helpers::stage_response::parse_stage_info;
use crate::stage_api::constants::{QUERY_IN_PROGRESS, SESSION_EXPIRED};
use crate::time_text::main::now_ms::now_ms;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Temporary or static AWS credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsCredentials {
    pub key_id: String,
    pub secret: Secret,
    pub session_token: Option<Secret>,
}

/// The key Snowflake hands out for client-side encryption of one stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionMaterial {
    pub query_stage_master_key: Secret,
    pub query_id: String,
    pub smk_id: i64,
}

/// The per-file encryption metadata sent as S3 user metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionMetadata {
    pub key: String,
    pub iv: String,
    pub matdesc: String,
}

/// An S3 bucket location and how to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTarget {
    pub endpoint: String,
    pub bucket: String,
    pub path_style: bool,
    pub prefix: String,
    pub region: String,
    pub credentials: AwsCredentials,
}

impl ObjectTarget {
    /// The host the request is signed for and the request URL path for `key`.
    pub fn host_and_path(&self, key: &str) -> (String, String) {
        let host = self
            .endpoint
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        let object = format!("{}{}", self.prefix, key.trim_start_matches('/'));
        match self.path_style {
            true => (host, format!("/{}/{object}", self.bucket)),
            false => (format!("{}.{host}", self.bucket), format!("/{object}")),
        }
    }

    /// `http` or `https`.
    pub fn scheme(&self) -> &'static str {
        match self.endpoint.starts_with("http://") {
            true => "http",
            false => "https",
        }
    }
}

/// An internal stage's S3 location, temporary credentials and encryption key, from a `PUT` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageInfo {
    pub target: ObjectTarget,
    pub credentials_expire_ms: Option<i64>,
    pub encryption: EncryptionMaterial,
}

/// What an object upload did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Uploaded,
    CredentialsExpired,
}

/// What an object existence check found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectPresence {
    Found,
    Missing,
    CredentialsExpired,
}

/// A Snowpipe load report: `insertReport` events or a `loadHistoryScan` page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub files: Vec<FileLoad>,
    pub next_begin_mark: Option<String>,
    /// False when events were missed or, for a history scan, more pages follow.
    pub complete: bool,
    /// The newest entry of a history scan page, where the next page starts.
    pub range_end: Option<String>,
}

/// One file's load, by its path relative to the pipe's location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLoad {
    pub path: String,
    pub status: LoadStatus,
    pub rows_inserted: u64,
    pub rows_parsed: u64,
    pub first_error: Option<String>,
}

/// A file's load status; unknown statuses count as in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadStatus {
    InProgress,
    Loaded,
    Failed,
    PartiallyLoaded,
}

#[derive(Debug)]
struct Tokens {
    session: String,
    master: String,
    validity: Duration,
    issued: Instant,
}

/// A logged-in driver session, renewed with its master token and re-logged-in when that expires.
#[derive(Debug)]
pub struct DriverSession {
    http: reqwest::Client,
    base_url: String,
    auth: Arc<Authenticator>,
    destination: DestinationConfig,
    tokens: Mutex<Option<Tokens>>,
    /// Numbers the session's requests: Snowflake expects each to have its own.
    sequence: SequenceIds,
}

/// Request numbers unique within a session, from 1, however many tasks share it.
#[derive(Debug, Default)]
pub struct SequenceIds(AtomicU64);

impl SequenceIds {
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }
}

impl DriverSession {
    pub fn new(
        http: reqwest::Client,
        auth: Arc<Authenticator>,
        destination: &DestinationConfig,
    ) -> Self {
        Self {
            http,
            base_url: account_url(destination),
            auth,
            destination: destination.clone(),
            tokens: Mutex::new(None),
            sequence: SequenceIds::default(),
        }
    }

    /// Upload location, temporary credentials and encryption material for `stage`, from a `PUT` that sends no bytes.
    pub async fn stage_info(&self, stage: &str) -> Result<StageInfo, ApiError> {
        let statement = format!(
            "PUT 'file:///sqlload/placeholder.json.gz' @{stage} AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE"
        );
        let data = self.query(&statement).await?;
        parse_stage_info(&data).map_err(ApiError::Protocol)
    }

    async fn query(&self, statement: &str) -> Result<Value, ApiError> {
        let mut renewed = false;
        loop {
            let token = self.session_token().await?;
            let body = json!({
                "sqlText": statement,
                "asyncExec": false,
                "sequenceId": self.sequence.next(),
                "querySubmissionTime": now_ms(),
            });
            let mut response = self
                .post_json("/queries/v1/query-request", &token, &body)
                .await?;
            if response["code"].as_str() == Some(SESSION_EXPIRED) && !renewed {
                renewed = true;
                self.renew(&token).await?;
                continue;
            }
            while QUERY_IN_PROGRESS.contains(&response["code"].as_str().unwrap_or_default()) {
                let url = response["data"]["getResultUrl"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                tokio::time::sleep(Duration::from_millis(250)).await;
                response = self.result_json(&url, &token).await?;
            }
            if !response["success"].as_bool().unwrap_or(false) {
                return Err(ApiError::Status {
                    status: 200,
                    code: response["code"].as_str().unwrap_or_default().to_owned(),
                    message: response["message"].as_str().unwrap_or_default().to_owned(),
                });
            }
            return Ok(response["data"].take());
        }
    }

    async fn session_token(&self) -> Result<String, ApiError> {
        let mut guard = self.tokens.lock().await;
        let usable = guard.as_ref().is_some_and(|tokens| {
            tokens.validity.is_zero() || tokens.issued.elapsed() < tokens.validity.mul_f64(0.9)
        });
        if !usable {
            *guard = Some(self.login().await?);
        }
        Ok(guard
            .as_ref()
            .map(|tokens| tokens.session.clone())
            .unwrap_or_default())
    }

    async fn login(&self) -> Result<Tokens, ApiError> {
        let login = self.auth.login_token()?;
        let body = json!({ "data": {
            "CLIENT_APP_ID": "sqlload",
            "CLIENT_APP_VERSION": env!("CARGO_PKG_VERSION"),
            "ACCOUNT_NAME": self.destination.account,
            "LOGIN_NAME": self.destination.user,
            "AUTHENTICATOR": login.authenticator,
            "TOKEN": login.token,
            "CLIENT_ENVIRONMENT": { "APPLICATION": "sqlload", "OS": std::env::consts::OS },
        }});
        let mut query = vec![
            ("request_id", uuid::Uuid::new_v4().to_string()),
            ("databaseName", self.destination.database.clone()),
            ("schemaName", self.destination.schema.clone()),
        ];
        query.extend(
            self.destination
                .warehouse
                .clone()
                .map(|warehouse| ("warehouse", warehouse)),
        );
        query.extend(self.destination.role.clone().map(|role| ("roleName", role)));
        let response: Value = self
            .http
            .post(format!("{}/session/v1/login-request", self.base_url))
            .query(&query)
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !response["success"].as_bool().unwrap_or(false) {
            return Err(ApiError::Auth(format!(
                "login failed: {} {}",
                response["code"],
                response["message"].as_str().unwrap_or_default()
            )));
        }
        let data = &response["data"];
        Ok(Tokens {
            session: data["token"].as_str().unwrap_or_default().to_owned(),
            master: data["masterToken"].as_str().unwrap_or_default().to_owned(),
            validity: Duration::from_secs(data["validityInSeconds"].as_u64().unwrap_or_default()),
            issued: Instant::now(),
        })
    }

    async fn renew(&self, rejected: &str) -> Result<(), ApiError> {
        let mut guard = self.tokens.lock().await;
        let Some(tokens) = guard.as_mut().filter(|tokens| tokens.session == rejected) else {
            return Ok(());
        };
        let body = json!({ "oldSessionToken": tokens.session, "requestType": "RENEW" });
        let response = self
            .post_json("/session/token-request", &tokens.master, &body)
            .await?;
        if !response["success"].as_bool().unwrap_or(false) {
            *guard = Some(self.login().await?);
            return Ok(());
        }
        let data = &response["data"];
        tokens.session = data["sessionToken"].as_str().unwrap_or_default().to_owned();
        if let Some(master) = data["masterToken"].as_str() {
            tokens.master = master.to_owned();
        }
        if let Some(seconds) = data["validityInSecondsST"].as_u64() {
            tokens.validity = Duration::from_secs(seconds);
        }
        tokens.issued = Instant::now();
        Ok(())
    }

    async fn post_json(&self, path: &str, token: &str, body: &Value) -> Result<Value, ApiError> {
        Ok(self
            .http
            .post(format!("{}{path}", self.base_url))
            .query(&[("requestId", uuid::Uuid::new_v4().to_string())])
            .header("Accept", "application/json")
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn result_json(&self, path: &str, token: &str) -> Result<Value, ApiError> {
        Ok(self
            .http
            .get(format!("{}{path}", self.base_url))
            .header("Accept", "application/json")
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

/// A Snowpipe and how to reach it.
#[derive(Debug, Clone, Copy)]
pub struct PipeTarget<'a> {
    pub http: &'a reqwest::Client,
    pub account_url: &'a str,
    pub auth: &'a Authenticator,
    pub pipe: &'a str,
}

/// One object upload or lookup: where, under which key, and the extra signed headers.
#[derive(Debug, Clone, Copy)]
pub struct ObjectUpload<'a> {
    pub target: &'a ObjectTarget,
    pub key: &'a str,
    pub headers: &'a BTreeMap<String, String>,
}
