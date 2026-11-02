//! Just enough of the Snowflake driver REST protocol: PAT login, query execution with
//! session-token renewal, stage metadata via `PUT`, logout, and Snowpipe `insertFiles`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};

use crate::encrypt::EncryptionMaterial;
use crate::util::{now_millis, uuid_v4};

const SESSION_EXPIRED: &str = "390112";
const QUERY_IN_PROGRESS: &str = "333333";
const QUERY_IN_PROGRESS_ASYNC: &str = "333334";
const CLIENT_APP_ID: &str = "sqlload-spike";
const USER_AGENT: &str = concat!("sqlload-spike/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug)]
pub struct Config {
    pub account: String,
    pub host: String,
    pub user: String,
    pub pat: String,
    pub role: String,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
}

struct Tokens {
    session: String,
    master: String,
    session_validity: Duration,
    master_validity: Duration,
    issued: Instant,
}

pub struct Session {
    http: reqwest::Client,
    cfg: Config,
    tokens: RwLock<Option<Tokens>>,
    renew_lock: Mutex<()>,
    sequence: AtomicU64,
    pub renewals: AtomicUsize,
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub data: Value,
}

impl QueryResult {
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(name))
    }
}

impl Session {
    pub fn new(http: reqwest::Client, cfg: Config) -> Self {
        Self {
            http,
            cfg,
            tokens: RwLock::new(None),
            renew_lock: Mutex::new(()),
            sequence: AtomicU64::new(1),
            renewals: AtomicUsize::new(0),
        }
    }

    fn url(&self, path_and_query: &str) -> String {
        format!("https://{}{}", self.cfg.host, path_and_query)
    }

    pub async fn login(&self) -> Result<()> {
        let body = json!({
            "data": {
                "CLIENT_APP_ID": CLIENT_APP_ID,
                "CLIENT_APP_VERSION": env!("CARGO_PKG_VERSION"),
                "ACCOUNT_NAME": self.cfg.account,
                "LOGIN_NAME": self.cfg.user,
                "AUTHENTICATOR": "PROGRAMMATIC_ACCESS_TOKEN",
                "TOKEN": self.cfg.pat,
                "CLIENT_ENVIRONMENT": { "APPLICATION": CLIENT_APP_ID, "OS": std::env::consts::OS },
            }
        });
        let q = [
            ("request_id", uuid_v4()),
            ("databaseName", self.cfg.database.clone()),
            ("schemaName", self.cfg.schema.clone()),
            ("warehouse", self.cfg.warehouse.clone()),
            ("roleName", self.cfg.role.clone()),
        ];
        let resp: Value = self
            .http
            .post(self.url("/session/v1/login-request"))
            .query(&q)
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("user-agent", USER_AGENT)
            .body(body.to_string())
            .send()
            .await?
            .error_for_status()?
            .json_value()
            .await?;
        if !resp["success"].as_bool().unwrap_or(false) {
            bail!(
                "login failed: code {} {}",
                resp["code"],
                resp["message"].as_str().unwrap_or("")
            );
        }
        let d = &resp["data"];
        let tokens = Tokens {
            session: str_field(d, "token")?,
            master: str_field(d, "masterToken")?,
            session_validity: Duration::from_secs(d["validityInSeconds"].as_u64().unwrap_or(0)),
            master_validity: Duration::from_secs(
                d["masterValidityInSeconds"].as_u64().unwrap_or(0),
            ),
            issued: Instant::now(),
        };
        eprintln!(
            "login ok: session token valid {}s, master token valid {}s",
            tokens.session_validity.as_secs(),
            tokens.master_validity.as_secs()
        );
        *self.tokens.write().await = Some(tokens);
        Ok(())
    }

    async fn session_token(&self) -> Result<String> {
        let guard = self.tokens.read().await;
        let t = guard.as_ref().ok_or_else(|| anyhow!("not logged in"))?;
        Ok(t.session.clone())
    }

    /// Renews the session token with the master token (`POST /session/token-request`).
    /// `seen` is the session token that was rejected, so concurrent callers renew once.
    pub async fn renew(&self, seen: Option<&str>) -> Result<()> {
        let _g = self.renew_lock.lock().await;
        let (old, master) = {
            let guard = self.tokens.read().await;
            let t = guard.as_ref().ok_or_else(|| anyhow!("not logged in"))?;
            if seen.is_some_and(|s| s != t.session) {
                return Ok(());
            }
            (t.session.clone(), t.master.clone())
        };
        let resp: Value = self
            .http
            .post(self.url("/session/token-request"))
            .query(&[("requestId", uuid_v4())])
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("user-agent", USER_AGENT)
            .header("authorization", format!("Snowflake Token=\"{master}\""))
            .body(json!({ "oldSessionToken": old, "requestType": "RENEW" }).to_string())
            .send()
            .await?
            .error_for_status()?
            .json_value()
            .await?;
        if !resp["success"].as_bool().unwrap_or(false) {
            // Master token expired too: a PAT can simply log in again.
            eprintln!(
                "session renewal failed (code {}); logging in again",
                resp["code"]
            );
            return self.login().await;
        }
        let d = &resp["data"];
        let mut guard = self.tokens.write().await;
        let t = guard.as_mut().expect("checked above");
        t.session = str_field(d, "sessionToken")?;
        if let Some(m) = d["masterToken"].as_str() {
            t.master = m.to_string();
        }
        if let Some(v) = d["validityInSecondsST"]
            .as_u64()
            .or(d["validityInSeconds"].as_u64())
        {
            t.session_validity = Duration::from_secs(v);
        }
        t.issued = Instant::now();
        self.renewals.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Renews proactively once most of the session token lifetime has passed.
    async fn maybe_renew_proactively(&self) -> Result<()> {
        let due = {
            let guard = self.tokens.read().await;
            match guard.as_ref() {
                Some(t) if !t.session_validity.is_zero() => {
                    t.issued.elapsed() > t.session_validity.mul_f64(0.9)
                }
                _ => false,
            }
        };
        if due { self.renew(None).await } else { Ok(()) }
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query_with_accept(sql, "application/snowflake").await
    }

    /// File-transfer commands must be sent with `Accept: application/json`, as the
    /// drivers do; `application/snowflake` is rejected for `PUT`.
    async fn query_with_accept(&self, sql: &str, accept: &str) -> Result<QueryResult> {
        self.maybe_renew_proactively().await?;
        let mut renewed = false;
        loop {
            let token = self.session_token().await?;
            let body = json!({
                "sqlText": sql,
                "asyncExec": false,
                "sequenceId": self.sequence.fetch_add(1, Ordering::Relaxed),
                "querySubmissionTime": now_millis() as u64,
            });
            let mut resp: Value = self
                .http
                .post(self.url("/queries/v1/query-request"))
                .query(&[("requestId", uuid_v4())])
                .header("content-type", "application/json")
                .header("accept", accept)
                .header("user-agent", USER_AGENT)
                .header("authorization", format!("Snowflake Token=\"{token}\""))
                .body(body.to_string())
                .send()
                .await?
                .error_for_status()?
                .json_value()
                .await?;
            if resp["code"].as_str() == Some(SESSION_EXPIRED) && !renewed {
                self.renew(Some(&token)).await?;
                renewed = true;
                continue;
            }
            while matches!(
                resp["code"].as_str(),
                Some(QUERY_IN_PROGRESS | QUERY_IN_PROGRESS_ASYNC)
            ) {
                let result_url = str_field(&resp["data"], "getResultUrl")?;
                tokio::time::sleep(Duration::from_millis(250)).await;
                resp = self
                    .http
                    .get(self.url(&result_url))
                    .header("accept", accept)
                    .header("user-agent", USER_AGENT)
                    .header("authorization", format!("Snowflake Token=\"{token}\""))
                    .send()
                    .await?
                    .error_for_status()?
                    .json_value()
                    .await?;
            }
            if !resp["success"].as_bool().unwrap_or(false) {
                bail!(
                    "query failed: code {} {}",
                    resp["code"],
                    resp["message"].as_str().unwrap_or("")
                );
            }
            let data = resp["data"].take();
            let columns = data["rowtype"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|c| c["name"].as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default();
            let rows = match data["rowset"].as_array() {
                Some(r) => r
                    .iter()
                    .map(|row| row.as_array().cloned().unwrap_or_default())
                    .collect(),
                None if data["rowsetBase64"].as_str().is_some_and(|s| !s.is_empty()) => {
                    bail!("server returned an Arrow result set; JSON was requested")
                }
                None => Vec::new(),
            };
            return Ok(QueryResult {
                columns,
                rows,
                data,
            });
        }
    }

    pub async fn logout(&self) -> Result<()> {
        let token = self.session_token().await?;
        self.http
            .post(self.url("/session"))
            .query(&[("delete", "true"), ("requestId", &uuid_v4())])
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("user-agent", USER_AGENT)
            .header("authorization", format!("Snowflake Token=\"{token}\""))
            .body("{}")
            .send()
            .await?
            .error_for_status()?;
        *self.tokens.write().await = None;
        Ok(())
    }

    /// Runs a `PUT` purely to obtain the stage location, temporary credentials and
    /// encryption material; the driver protocol never sends file bytes to Snowflake.
    pub async fn stage_info(&self, stage: &str) -> Result<StageInfo> {
        let sql = format!(
            "PUT 'file:///sqlload/placeholder.json.gz' @{stage} \
             AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=GZIP OVERWRITE=TRUE"
        );
        let r = self.query_with_accept(&sql, "application/json").await?;
        if std::env::var_os("SQLLOAD_DEBUG").is_some() {
            // Field names only; values contain credentials.
            let keys = |v: &Value| v.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>());
            eprintln!("PUT data keys: {:?}", keys(&r.data));
            eprintln!("PUT stageInfo keys: {:?}", keys(&r.data["stageInfo"]));
            eprintln!("PUT creds keys: {:?}", keys(&r.data["stageInfo"]["creds"]));
            let si = &r.data["stageInfo"];
            for k in [
                "credExpiryTime",
                "ciphers",
                "isClientSideEncrypted",
                "useS3RegionalUrl",
                "useVirtualUrl",
                "volumeEncryptionMode",
            ] {
                eprintln!("PUT stageInfo.{k} = {}", si[k]);
            }
            for k in [
                "threshold",
                "parallel",
                "autoCompress",
                "sourceCompression",
                "clientShowEncryptionParameter",
            ] {
                eprintln!("PUT {k} = {}", r.data[k]);
            }
        }
        StageInfo::from_put_response(&r.data)
    }

    /// Snowpipe REST `insertFiles`, authenticated with the PAT as a bearer token.
    pub async fn insert_files(&self, pipe: &str, paths: &[String]) -> Result<Value> {
        let files: Vec<Value> = paths.iter().map(|p| json!({ "path": p })).collect();
        let resp = self
            .http
            .post(self.url(&format!("/v1/data/pipes/{pipe}/insertFiles")))
            .query(&[("requestId", uuid_v4())])
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("user-agent", USER_AGENT)
            .header("authorization", format!("Bearer {}", self.cfg.pat))
            .header(
                "x-snowflake-authorization-token-type",
                "PROGRAMMATIC_ACCESS_TOKEN",
            )
            .body(json!({ "files": files }).to_string())
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!(
                "insertFiles failed: HTTP {status}: {}",
                text.chars().take(300).collect::<String>()
            );
        }
        Ok(serde_json::from_str(&text)?)
    }
}

#[derive(Clone, Debug)]
pub struct AwsCreds {
    pub key_id: String,
    pub secret: String,
    pub token: String,
}

#[derive(Clone, Debug)]
pub struct StageInfo {
    pub bucket: String,
    /// Object key prefix for the stage, ending in `/` (or empty).
    pub path_prefix: String,
    pub region: String,
    pub end_point: Option<String>,
    pub creds: AwsCreds,
    /// `stageInfo.credExpiryTime` (Unix milliseconds), about two hours after the `PUT`.
    pub cred_expiry_ms: Option<u64>,
    pub encryption: EncryptionMaterial,
}

impl StageInfo {
    pub fn from_put_response(data: &Value) -> Result<Self> {
        let si = &data["stageInfo"];
        let location_type = si["locationType"].as_str().unwrap_or("");
        if location_type != "S3" {
            bail!("only S3-backed internal stages are supported, got {location_type:?}");
        }
        let location = str_field(si, "location")?;
        let (bucket, path) = location.split_once('/').unwrap_or((&location, ""));
        let mut path_prefix = path.to_string();
        if !path_prefix.is_empty() && !path_prefix.ends_with('/') {
            path_prefix.push('/');
        }
        let creds = &si["creds"];
        // encryptionMaterial is an object for uploads, a list (one per file) for downloads.
        let em = match &data["encryptionMaterial"] {
            Value::Array(a) => a.first().cloned().unwrap_or(Value::Null),
            v => v.clone(),
        };
        if em.is_null() {
            bail!("stage is not client-side encrypted; this spike expects SNOWFLAKE_FULL");
        }
        Ok(Self {
            bucket: bucket.to_string(),
            path_prefix,
            region: str_field(si, "region")?,
            end_point: si["endPoint"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(String::from),
            creds: AwsCreds {
                key_id: str_field(creds, "AWS_KEY_ID")?,
                secret: str_field(creds, "AWS_SECRET_KEY")?,
                token: str_field(creds, "AWS_TOKEN")?,
            },
            cred_expiry_ms: si["credExpiryTime"].as_u64(),
            encryption: EncryptionMaterial {
                query_stage_master_key: str_field(&em, "queryStageMasterKey")?,
                query_id: str_field(&em, "queryId")?,
                smk_id: em["smkId"]
                    .as_i64()
                    .or_else(|| em["smkId"].as_str().and_then(|s| s.parse().ok()))
                    .context("smkId")?,
            },
        })
    }

    /// True when the credentials expire within `margin`.
    pub fn creds_expiring(&self, margin: Duration) -> bool {
        self.cred_expiry_ms
            .is_some_and(|exp| (now_millis() as u64) + margin.as_millis() as u64 >= exp)
    }

    /// Virtual-hosted endpoint. The drivers use the global endpoint unless told to use a
    /// regional one; the regional endpoint is always valid and avoids cross-region redirects.
    pub fn s3_host(&self) -> String {
        match &self.end_point {
            Some(ep) => format!("{}.{ep}", self.bucket),
            None if self.region.starts_with("cn-") => {
                format!("{}.s3.{}.amazonaws.com.cn", self.bucket, self.region)
            }
            None => format!("{}.s3.{}.amazonaws.com", self.bucket, self.region),
        }
    }
}

fn str_field(v: &Value, name: &str) -> Result<String> {
    v[name]
        .as_str()
        .map(String::from)
        .ok_or_else(|| anyhow!("response field {name} missing"))
}

trait JsonValue {
    async fn json_value(self) -> Result<Value>;
}

impl JsonValue for reqwest::Response {
    async fn json_value(self) -> Result<Value> {
        let bytes = self.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_put_response() {
        let data = json!({
            "command": "UPLOAD",
            "stageInfo": {
                "locationType": "S3",
                "location": "example-bucket/stages/abc123",
                "region": "us-west-2",
                "credExpiryTime": 1_000_000u64,
                "endPoint": "",
                "creds": { "AWS_KEY_ID": "AKIDEXAMPLE", "AWS_SECRET_KEY": "secret", "AWS_TOKEN": "token" }
            },
            "encryptionMaterial": { "queryStageMasterKey": "AAAAAAAAAAAAAAAAAAAAAA==", "queryId": "q1", "smkId": 42 }
        });
        let s = StageInfo::from_put_response(&data).unwrap();
        assert_eq!(s.bucket, "example-bucket");
        assert_eq!(s.path_prefix, "stages/abc123/");
        assert_eq!(s.s3_host(), "example-bucket.s3.us-west-2.amazonaws.com");
        assert_eq!(s.encryption.smk_id, 42);
        assert!(s.creds_expiring(Duration::ZERO));
    }
}
