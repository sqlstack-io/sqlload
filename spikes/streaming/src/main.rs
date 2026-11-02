//! Spike: Snowpipe Streaming high-performance REST API with Named Channels, using only
//! `reqwest` (no Snowflake SDK, no driver crate).
//!
//! All account details come from environment variables; see `README.md`.

mod conn_count;

use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::{Client, Response, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const PAT_TOKEN_TYPE: &str = "PROGRAMMATIC_ACCESS_TOKEN";
/// Hard per-request cap documented for append requests (bytes on the wire).
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq)]
enum AuthMode {
    /// Send the PAT itself to the ingest host.
    Pat,
    /// Exchange the PAT for a token scoped to the ingest host.
    Scoped,
}

struct Config {
    account: String,
    pat: String,
    role: Option<String>,
    warehouse: Option<String>,
    database: String,
    schema: String,
    table: String,
    pipe: String,
    channel: String,
    auth: AuthMode,
}

fn required(name: &str) -> Res<String> {
    env::var(name).map_err(|_| format!("missing env var {name}").into())
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

impl Config {
    fn from_env() -> Res<Self> {
        let table = required("SQLLOAD_SNOWFLAKE_TABLE")?;
        let pipe =
            optional("SQLLOAD_SNOWFLAKE_PIPE").unwrap_or_else(|| format!("{table}-STREAMING"));
        let auth = match optional("SQLLOAD_AUTH").as_deref() {
            None | Some("pat") => AuthMode::Pat,
            Some("scoped") => AuthMode::Scoped,
            Some(other) => {
                return Err(format!("SQLLOAD_AUTH must be pat|scoped, got {other}").into());
            }
        };
        Ok(Self {
            account: required("SQLLOAD_SNOWFLAKE_ACCOUNT")?.to_lowercase(),
            pat: required("SQLLOAD_SNOWFLAKE_PAT")?,
            role: optional("SQLLOAD_SNOWFLAKE_ROLE"),
            warehouse: optional("SQLLOAD_SNOWFLAKE_WAREHOUSE"),
            database: required("SQLLOAD_SNOWFLAKE_DATABASE")?,
            schema: required("SQLLOAD_SNOWFLAKE_SCHEMA")?,
            table,
            pipe,
            channel: optional("SQLLOAD_CHANNEL").unwrap_or_else(|| "spike_channel_0".into()),
            auth,
        })
    }
}

#[derive(Debug, Deserialize, Default)]
#[allow(dead_code)]
struct ChannelStatus {
    channel_status_code: Option<String>,
    last_committed_offset_token: Option<String>,
    rows_inserted: Option<i64>,
    rows_parsed: Option<i64>,
    #[serde(alias = "rows_errors")]
    rows_error_count: Option<i64>,
    last_error_offset_upper_bound: Option<String>,
    last_error_message: Option<String>,
    snowflake_avg_processing_latency_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OpenResponse {
    next_continuation_token: String,
    channel_status: ChannelStatus,
}

#[derive(Debug, Deserialize)]
struct AppendResponse {
    next_continuation_token: String,
}

#[derive(Debug, Deserialize)]
struct BulkStatusResponse {
    channel_statuses: HashMap<String, ChannelStatus>,
}

/// Error returned by a Snowflake endpoint, kept structured so callers can branch on it.
#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: String,
    body: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HTTP {} code={} body={}",
            self.status, self.code, self.body
        )
    }
}

impl std::error::Error for ApiError {}

async fn check(resp: Response) -> Res<Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let code = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v.get("code").and_then(|c| c.as_str()).map(str::to_owned))
        .unwrap_or_default();
    let body: String = body.chars().take(500).collect();
    Err(Box::new(ApiError { status, code, body }))
}

fn api_error<'a>(e: &'a (dyn std::error::Error + Send + Sync + 'static)) -> Option<&'a ApiError> {
    e.downcast_ref::<ApiError>()
}

struct Streamer {
    http: Client,
    cfg: Config,
    connections: Arc<AtomicUsize>,
    ingest_host: String,
    ingest_token: String,
}

impl Streamer {
    async fn connect(cfg: Config) -> Res<Self> {
        let connections = Arc::new(AtomicUsize::new(0));
        let http = Client::builder()
            .connector_layer(conn_count::CountingLayer::new(connections.clone()))
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .user_agent(concat!(
                "sqlload-spike-streaming/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()?;

        let control = format!("https://{}.snowflakecomputing.com", cfg.account);
        let resp = http
            .get(format!("{control}/v2/streaming/hostname"))
            .bearer_auth(&cfg.pat)
            .header("X-Snowflake-Authorization-Token-Type", PAT_TOKEN_TYPE)
            .send()
            .await?;
        // Documented workaround: underscores in the returned host break the ingest service.
        let ingest_host = check(resp).await?.text().await?.trim().replace('_', "-");
        eprintln!("ingest host discovered ({} chars)", ingest_host.len());

        let ingest_token = match cfg.auth {
            AuthMode::Pat => cfg.pat.clone(),
            AuthMode::Scoped => {
                let resp = http
                    .post(format!("{control}/oauth/token"))
                    .form(&[
                        (
                            "grant_type",
                            "urn:ietf:params:oauth:grant-type:token-exchange",
                        ),
                        ("scope", ingest_host.as_str()),
                        ("subject_token", cfg.pat.as_str()),
                        ("subject_token_type", "programmatic_access_token"),
                    ])
                    .send()
                    .await?;
                let body = check(resp).await?.text().await?;
                let token = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|v| {
                        ["access_token", "token"]
                            .iter()
                            .find_map(|k| v.get(*k).and_then(|t| t.as_str()).map(str::to_owned))
                    })
                    .unwrap_or_else(|| body.trim().to_owned());
                eprintln!("scoped token obtained ({} chars)", token.len());
                token
            }
        };

        Ok(Self {
            http,
            cfg,
            connections,
            ingest_host,
            ingest_token,
        })
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn ingest(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let req = self
            .http
            .request(method, format!("https://{}{}", self.ingest_host, path))
            .bearer_auth(&self.ingest_token)
            .query(&[("requestId", uuid::Uuid::new_v4().to_string())]);
        match self.cfg.auth {
            AuthMode::Pat => req.header("X-Snowflake-Authorization-Token-Type", PAT_TOKEN_TYPE),
            AuthMode::Scoped => req,
        }
    }

    /// `databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}`
    fn channel_path(&self, channel: &str) -> String {
        let c = &self.cfg;
        format!(
            "databases/{}/schemas/{}/pipes/{}/channels/{}",
            c.database, c.schema, c.pipe, channel
        )
    }

    /// Opens (or reopens) a Named Channel. With `fail_on_uncommitted_rows`, retries the
    /// documented 409 conflicts for up to two minutes so in-flight rows can commit first.
    async fn open_channel(&self, channel: &str) -> Res<OpenResponse> {
        let started = Instant::now();
        loop {
            let resp = self
                .ingest(
                    reqwest::Method::PUT,
                    &format!("/v2/streaming/{}", self.channel_path(channel)),
                )
                .json(&json!({ "fail_on_uncommitted_rows": true }))
                .send()
                .await?;
            match check(resp).await {
                Ok(resp) => {
                    eprintln!("open channel: negotiated {:?}", resp.version());
                    return Ok(resp.json().await?);
                }
                Err(e) => {
                    let retry = api_error(e.as_ref()).is_some_and(|a| {
                        a.status == StatusCode::CONFLICT
                            && matches!(
                                a.code.as_str(),
                                "ERR_CHANNEL_HAS_UNCOMMITTED_DATA" | "ERR_OPEN_CHANNEL_IN_PROGRESS"
                            )
                    });
                    if !retry || started.elapsed() > Duration::from_secs(120) {
                        return Err(e);
                    }
                    eprintln!("open: {e}; retrying");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// Appends one gzip'd NDJSON body. Returns the next continuation token and wire size.
    async fn append(
        &self,
        channel: &str,
        continuation: &str,
        offset_token: &str,
        ndjson: &[u8],
    ) -> Res<(String, usize)> {
        let body = gzip(ndjson)?;
        let wire = body.len();
        let resp = self
            .ingest(
                reqwest::Method::POST,
                &format!("/v2/streaming/data/{}/rows", self.channel_path(channel)),
            )
            .query(&[
                ("continuationToken", continuation),
                ("offsetToken", offset_token),
            ])
            .header("Content-Type", "application/x-ndjson")
            .header("Content-Encoding", "gzip")
            .body(body)
            .send()
            .await?;
        let parsed: AppendResponse = check(resp).await?.json().await?;
        Ok((parsed.next_continuation_token, wire))
    }

    async fn status(&self, channel: &str) -> Res<ChannelStatus> {
        let c = &self.cfg;
        let path = format!(
            "/v2/streaming/databases/{}/schemas/{}/pipes/{}:bulk-channel-status",
            c.database, c.schema, c.pipe
        );
        let resp = self
            .ingest(reqwest::Method::POST, &path)
            .json(&json!({ "channel_names": [channel] }))
            .send()
            .await?;
        let mut parsed: BulkStatusResponse = check(resp).await?.json().await?;
        // Channel names are matched case-insensitively on open but echoed upper-cased.
        let key = parsed
            .channel_statuses
            .keys()
            .find(|k| k.eq_ignore_ascii_case(channel))
            .cloned();
        Ok(key
            .and_then(|k| parsed.channel_statuses.remove(&k))
            .unwrap_or_default())
    }

    /// Polls channel status until `last_committed_offset_token >= target`.
    async fn wait_committed(
        &self,
        channel: &str,
        target: u64,
        timeout: Duration,
    ) -> Res<ChannelStatus> {
        let started = Instant::now();
        loop {
            let st = self.status(channel).await?;
            if parse_offset(st.last_committed_offset_token.as_deref()).is_some_and(|o| o >= target)
            {
                return Ok(st);
            }
            if started.elapsed() > timeout {
                return Err(format!("timed out waiting for offset {target}: {st:?}").into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Runs one statement through the SQL API (`/api/v2/statements`) with the PAT, on the
    /// same pooled client. Returns rows as strings.
    async fn sql(&self, statement: &str) -> Res<Vec<Vec<Option<String>>>> {
        let c = &self.cfg;
        let control = format!("https://{}.snowflakecomputing.com", c.account);
        let mut body = json!({
            "statement": statement,
            "timeout": 60,
            "database": c.database,
            "schema": c.schema,
        });
        if let Some(w) = &c.warehouse {
            body["warehouse"] = json!(w);
        }
        if let Some(r) = &c.role {
            body["role"] = json!(r);
        }
        let resp = self
            .http
            .post(format!("{control}/api/v2/statements"))
            .bearer_auth(&c.pat)
            .header("X-Snowflake-Authorization-Token-Type", PAT_TOKEN_TYPE)
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await?;
        let mut resp = check(resp).await?;
        loop {
            let status = resp.status();
            let v: Value = resp.json().await?;
            if status != StatusCode::ACCEPTED {
                let rows = v["data"]
                    .as_array()
                    .map(|rows| {
                        rows.iter()
                            .map(|r| {
                                r.as_array()
                                    .map(|cells| {
                                        cells
                                            .iter()
                                            .map(|c| c.as_str().map(str::to_owned))
                                            .collect()
                                    })
                                    .unwrap_or_default()
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                return Ok(rows);
            }
            let handle = v["statementHandle"]
                .as_str()
                .ok_or("no statementHandle")?
                .to_owned();
            tokio::time::sleep(Duration::from_millis(250)).await;
            resp = check(
                self.http
                    .get(format!("{control}/api/v2/statements/{handle}"))
                    .bearer_auth(&c.pat)
                    .header("X-Snowflake-Authorization-Token-Type", PAT_TOKEN_TYPE)
                    .send()
                    .await?,
            )
            .await?;
        }
    }

    fn table_fqn(&self) -> String {
        format!(
            "{}.{}.{}",
            self.cfg.database, self.cfg.schema, self.cfg.table
        )
    }
}

fn parse_offset(token: Option<&str>) -> Option<u64> {
    token.and_then(|t| t.parse().ok())
}

fn gzip(data: &[u8]) -> Res<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::with_capacity(data.len() / 4), Compression::default());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// One NDJSON line of neutral example data for a fake source offset.
fn order_row(offset: u64, channel: &str) -> String {
    let row = json!({
        "kafka_offset": offset,
        "order_id": 100_000 + offset,
        "customer": format!("customer-{}", offset % 17),
        "amount": format!("{}.{:02}", offset % 500, offset % 100),
        "channel": channel,
        "sent_at_ms": now_ms() as u64,
        "payload": { "sku": format!("SKU-{}", offset % 23), "qty": offset % 5 + 1 },
    });
    let mut s = row.to_string();
    s.push('\n');
    s
}

/// System-wide TCP active opens from `/proc/net/snmp` (Linux only, noisy on a shared box).
fn tcp_active_opens() -> Option<u64> {
    let snmp = std::fs::read_to_string("/proc/net/snmp").ok()?;
    let mut lines = snmp.lines().filter(|l| l.starts_with("Tcp:"));
    let header: Vec<&str> = lines.next()?.split_whitespace().collect();
    let values: Vec<&str> = lines.next()?.split_whitespace().collect();
    let idx = header.iter().position(|h| *h == "ActiveOpens")?;
    values.get(idx)?.parse().ok()
}

struct Args {
    command: String,
    appends: u64,
    batch: u64,
    wait: bool,
    verify: bool,
}

fn parse_args() -> Res<Args> {
    let mut args = Args {
        command: "run".into(),
        appends: 20,
        batch: 50,
        wait: true,
        verify: true,
    };
    let mut it = env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--appends" => args.appends = it.next().ok_or("--appends N")?.parse()?,
            "--batch" => args.batch = it.next().ok_or("--batch N")?.parse()?,
            "--no-wait" => args.wait = false,
            "--no-verify" => args.verify = false,
            "run" | "large-row" | "bad-row" => args.command = a,
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    Ok(args)
}

/// Open → resume from the last committed offset → append N batches → wait for commit →
/// verify with SQL. Running it twice proves restart-without-duplicates.
async fn cmd_run(s: &Streamer, args: &Args) -> Res<()> {
    let channel = s.cfg.channel.clone();
    let opens_before = tcp_active_opens();
    let t_open = Instant::now();
    let open = s.open_channel(&channel).await?;
    let committed = parse_offset(open.channel_status.last_committed_offset_token.as_deref());
    println!(
        "open: {:?} in {} ms, status={:?}, last_committed_offset_token={:?}",
        s.cfg.auth,
        t_open.elapsed().as_millis(),
        open.channel_status.channel_status_code,
        open.channel_status.last_committed_offset_token
    );
    let mut next_offset = committed.map_or(1, |o| o + 1);
    println!("resuming at fake source offset {next_offset}");

    let mut continuation = open.next_continuation_token;
    let mut wire_total = 0usize;
    let mut raw_total = 0usize;
    let mut last_offset = committed.unwrap_or(0);
    let t_appends = Instant::now();
    let mut last_append_done = Instant::now();
    for _ in 0..args.appends {
        let mut body = String::new();
        for _ in 0..args.batch {
            body.push_str(&order_row(next_offset, &channel));
            last_offset = next_offset;
            next_offset += 1;
        }
        raw_total += body.len();
        let (next, wire) = s
            .append(
                &channel,
                &continuation,
                &last_offset.to_string(),
                body.as_bytes(),
            )
            .await?;
        wire_total += wire;
        continuation = next;
        last_append_done = Instant::now();
    }
    let append_ms = t_appends.elapsed().as_millis();
    println!(
        "appended {} batches x {} rows (offsets up to {last_offset}) in {append_ms} ms; raw {raw_total} B, gzip {wire_total} B",
        args.appends, args.batch
    );
    println!("connections opened so far: {}", s.connections());

    if !args.wait {
        println!("--no-wait: exiting without waiting for commit (simulated crash)");
        report_connections(s, opens_before);
        return Ok(());
    }

    let st = s
        .wait_committed(&channel, last_offset, Duration::from_secs(300))
        .await?;
    let commit_ms = last_append_done.elapsed().as_millis();
    println!(
        "committed offset {last_offset} {commit_ms} ms after last append response; status={st:?}"
    );

    if args.verify {
        let q = format!(
            "SELECT COUNT(*), COUNT(DISTINCT kafka_offset), MIN(kafka_offset), MAX(kafka_offset) FROM {} WHERE channel = '{}'",
            s.table_fqn(),
            channel
        );
        loop {
            let rows = s.sql(&q).await?;
            let r = rows.first().cloned().unwrap_or_default();
            let max = r
                .get(3)
                .cloned()
                .flatten()
                .and_then(|m| m.parse::<u64>().ok());
            if max.is_some_and(|m| m >= last_offset) {
                println!(
                    "visible {} ms after last append response: count={:?} distinct={:?} min={:?} max={:?}",
                    last_append_done.elapsed().as_millis(),
                    r.first().cloned().flatten(),
                    r.get(1).cloned().flatten(),
                    r.get(2).cloned().flatten(),
                    max
                );
                break;
            }
            if last_append_done.elapsed() > Duration::from_secs(300) {
                return Err("rows did not become visible within 300 s".into());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    report_connections(s, opens_before);
    Ok(())
}

fn report_connections(s: &Streamer, opens_before: Option<u64>) {
    println!(
        "new TCP connections opened by the client (connector calls): {}",
        s.connections()
    );
    if let (Some(b), Some(a)) = (opens_before, tcp_active_opens()) {
        println!(
            "system-wide /proc/net/snmp Tcp ActiveOpens delta during run: {}",
            a - b
        );
    }
}

/// Builds one JSON row of roughly `target` bytes. `compressible` chooses between realistic
/// repeated-structure JSON and near-random hex text.
fn big_row(offset: u64, channel: &str, target: usize, compressible: bool) -> String {
    let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ offset;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut items = Vec::new();
    let mut size = 0;
    while size < target {
        let item = if compressible {
            let n = next();
            json!({
                "line_id": items.len(),
                "sku": format!("SKU-{}", n % 5000),
                "qty": n % 9 + 1,
                "unit_price": format!("{}.{:02}", n % 300, n % 100),
                "warehouse": format!("wh-{}", n % 7),
                "note": "standard shipping, gift wrap not requested",
            })
        } else {
            json!(format!(
                "{:016x}{:016x}{:016x}{:016x}",
                next(),
                next(),
                next(),
                next()
            ))
        };
        size += item.to_string().len() + 1;
        items.push(item);
    }
    let row = json!({
        "kafka_offset": offset,
        "order_id": offset,
        "customer": "bulk-customer",
        "amount": "1.00",
        "channel": channel,
        "sent_at_ms": now_ms() as u64,
        "payload": { "lines": items },
    });
    let mut s = row.to_string();
    s.push('\n');
    s
}

async fn cmd_large_row(s: &Streamer) -> Res<()> {
    let channel = s.cfg.channel.clone();
    let open = s.open_channel(&channel).await?;
    let mut offset =
        parse_offset(open.channel_status.last_committed_offset_token.as_deref()).unwrap_or(0);
    let mut continuation = open.next_continuation_token;
    for compressible in [true, false] {
        offset += 1;
        let row = big_row(offset, &channel, 9 * 1024 * 1024, compressible);
        let wire = gzip(row.as_bytes())?.len();
        println!(
            "large row (compressible={compressible}): raw {} B, gzip {} B ({:.1}% of 4 MiB limit)",
            row.len(),
            wire,
            100.0 * wire as f64 / MAX_REQUEST_BYTES as f64
        );
        let t = Instant::now();
        match s
            .append(&channel, &continuation, &offset.to_string(), row.as_bytes())
            .await
        {
            Ok((next, _)) => {
                continuation = next;
                println!("  append accepted in {} ms", t.elapsed().as_millis());
                match s
                    .wait_committed(&channel, offset, Duration::from_secs(180))
                    .await
                {
                    Ok(st) => println!(
                        "  committed {} ms after append; status={st:?}",
                        t.elapsed().as_millis()
                    ),
                    Err(e) => println!("  not committed: {e}"),
                }
            }
            Err(e) => {
                println!(
                    "  append rejected after {} ms: {e}",
                    t.elapsed().as_millis()
                );
                // Rejected requests may invalidate the continuation token; reopen to be safe.
                let open = s.open_channel(&channel).await?;
                continuation = open.next_continuation_token;
                offset = parse_offset(open.channel_status.last_committed_offset_token.as_deref())
                    .unwrap_or(0);
            }
        }
    }
    let st = s.status(&channel).await?;
    println!("final status: {st:?}");
    println!("connections opened: {}", s.connections());
    Ok(())
}

async fn cmd_bad_row(s: &Streamer) -> Res<()> {
    let channel = s.cfg.channel.clone();
    let open = s.open_channel(&channel).await?;
    let mut offset =
        parse_offset(open.channel_status.last_committed_offset_token.as_deref()).unwrap_or(0);
    let mut continuation = open.next_continuation_token;
    println!("open: status before={:?}", open.channel_status);

    type BadRow<'a> = Box<dyn Fn(u64) -> String + 'a>;
    let cases: Vec<(&str, BadRow)> = vec![
        (
            "type error (amount = \"not-a-number\")",
            Box::new(|o| {
                format!(
                    "{{\"kafka_offset\":{o},\"order_id\":{o},\"customer\":\"c\",\"amount\":\"not-a-number\",\"channel\":\"{channel}\"}}\n"
                )
            }),
        ),
        (
            "missing NOT NULL column (kafka_offset)",
            Box::new(|o| {
                format!(
                    "{{\"order_id\":{o},\"customer\":\"c\",\"amount\":\"1\",\"channel\":\"{channel}\"}}\n"
                )
            }),
        ),
        (
            "malformed JSON line",
            Box::new(|o| {
                format!("{{\"kafka_offset\":{o},\"order_id\": oops, \"channel\":\"{channel}\"\n")
            }),
        ),
    ];
    for (name, make_bad) in &cases {
        // Each batch: good row, bad row, good row.
        let body = format!(
            "{}{}{}",
            order_row(offset + 1, &channel),
            make_bad(offset + 2),
            order_row(offset + 3, &channel)
        );
        let token = (offset + 3).to_string();
        println!("case: {name}");
        match s
            .append(&channel, &continuation, &token, body.as_bytes())
            .await
        {
            Ok((next, _)) => {
                continuation = next;
                offset += 3;
                match s
                    .wait_committed(&channel, offset, Duration::from_secs(120))
                    .await
                {
                    Ok(st) => println!("  append 200; committed; status={st:?}"),
                    Err(e) => println!("  append 200; {e}"),
                }
            }
            Err(e) => {
                println!("  append rejected: {e}");
                let open = s.open_channel(&channel).await?;
                continuation = open.next_continuation_token;
                offset = parse_offset(open.channel_status.last_committed_offset_token.as_deref())
                    .unwrap_or(0);
                println!("  reopened; status={:?}", open.channel_status);
            }
        }
    }
    let q = format!(
        "SELECT COUNT(*), MIN(kafka_offset), MAX(kafka_offset) FROM {} WHERE channel = '{}'",
        s.table_fqn(),
        channel
    );
    println!("target rows for channel: {:?}", s.sql(&q).await?);
    let q = format!(
        "SELECT error_metadata:error_message::STRING, error_metadata:details:error_data_content_type::STRING, \
         error_metadata:details:offset_token_upper_bound::STRING, LEFT(error_data:\"$1\"::STRING, 200) \
         FROM ERROR_TABLE({}) WHERE error_metadata:details:channel_name::STRING ILIKE '{}' ORDER BY timestamp",
        s.table_fqn(),
        channel
    );
    match s.sql(&q).await {
        Ok(rows) => {
            println!("error table rows for channel: {}", rows.len());
            for r in rows {
                println!("  {r:?}");
            }
        }
        Err(e) => println!("error table query failed: {e}"),
    }
    println!("connections opened: {}", s.connections());
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Res<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "failed to install rustls ring provider")?;
    let args = parse_args()?;
    let cfg = Config::from_env()?;
    let started = Instant::now();
    let s = Streamer::connect(cfg).await?;
    match args.command.as_str() {
        "run" => cmd_run(&s, &args).await?,
        "large-row" => cmd_large_row(&s).await?,
        "bad-row" => cmd_bad_row(&s).await?,
        _ => unreachable!(),
    }
    println!("total {} ms", started.elapsed().as_millis());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_row_is_one_ndjson_line() {
        let row = order_row(42, "c");
        assert!(row.ends_with('\n'));
        assert_eq!(row.matches('\n').count(), 1);
        let v: Value = serde_json::from_str(row.trim_end()).unwrap();
        assert_eq!(v["kafka_offset"], 42);
    }

    #[test]
    fn big_row_reaches_target_size() {
        let row = big_row(1, "c", 1 << 20, true);
        assert!(row.len() >= 1 << 20);
        assert!(serde_json::from_str::<Value>(row.trim_end()).is_ok());
    }

    #[test]
    fn gzip_round_trips() {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let data = b"{\"a\":1}\n".repeat(100);
        let mut out = Vec::new();
        GzDecoder::new(&gzip(&data).unwrap()[..])
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn parse_offset_handles_missing_and_bad_tokens() {
        assert_eq!(parse_offset(Some("17")), Some(17));
        assert_eq!(parse_offset(Some("x")), None);
        assert_eq!(parse_offset(None), None);
    }
}
