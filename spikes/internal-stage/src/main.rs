mod conn_count;
mod encrypt;
mod s3;
mod snowflake;
mod util;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use conn_count::{ConnCounter, tcp_active_opens};
use s3::PutOutcome;
use snowflake::{Config, Session, StageInfo};

const USAGE: &str = "\
usage:
  spike-internal-stage run [--files N] [--rows R] [--concurrency C]
                           [--stage-creds-ttl SECS] [--fresh-s3-client]
                           [--renew-session] [--skip-copy]
  spike-internal-stage encrypt-fixture <out-dir>

options for `run`:
  --stage-creds-ttl SECS  also refetch stage credentials after SECS (0 = every file)
  --fresh-s3-client       new HTTPS client per file, to compare with per-file clients
  --renew-session         renew the session token once before COPY

environment for `run`:
  SQLLOAD_SNOWFLAKE_ACCOUNT    account identifier, e.g. myorg-myaccount
  SQLLOAD_SNOWFLAKE_HOST       optional, default <account>.snowflakecomputing.com
  SQLLOAD_SNOWFLAKE_USER, SQLLOAD_SNOWFLAKE_PAT, SQLLOAD_SNOWFLAKE_ROLE,
  SQLLOAD_SNOWFLAKE_WAREHOUSE, SQLLOAD_SNOWFLAKE_DATABASE, SQLLOAD_SNOWFLAKE_SCHEMA
  SQLLOAD_STAGE                internal stage name (in database.schema)
  SQLLOAD_TABLE                target table with a single VARIANT column
  SQLLOAD_PIPE, SQLLOAD_PIPE_TABLE
                               optional: pipe defined as COPY INTO <pipe table>
                               FROM @<stage> FILE_FORMAT=(TYPE=JSON), for insertFiles
";

struct Args {
    files: usize,
    rows: usize,
    concurrency: usize,
    stage_creds_ttl: Duration,
    fresh_s3_client: bool,
    renew_session: bool,
    skip_copy: bool,
}

fn parse_args(mut it: impl Iterator<Item = String>) -> Result<Args> {
    let mut a = Args {
        files: 50,
        rows: 100,
        concurrency: 1,
        stage_creds_ttl: Duration::MAX,
        fresh_s3_client: false,
        renew_session: false,
        skip_copy: false,
    };
    while let Some(flag) = it.next() {
        let mut num = |name: &str| -> Result<u64> {
            it.next()
                .with_context(|| format!("{name} needs a value"))?
                .parse()
                .with_context(|| format!("{name} must be a number"))
        };
        match flag.as_str() {
            "--files" => a.files = num("--files")? as usize,
            "--rows" => a.rows = num("--rows")? as usize,
            "--concurrency" => a.concurrency = (num("--concurrency")? as usize).max(1),
            "--stage-creds-ttl" => {
                a.stage_creds_ttl = Duration::from_secs(num("--stage-creds-ttl")?)
            }
            "--fresh-s3-client" => a.fresh_s3_client = true,
            "--renew-session" => a.renew_session = true,
            "--skip-copy" => a.skip_copy = true,
            other => bail!("unknown argument {other}\n{USAGE}"),
        }
    }
    Ok(a)
}

fn env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is not set"))
}

fn http_client(counter: &ConnCounter) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connector_layer(counter.clone())
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(300))
        .build()?)
}

/// Stage location, temporary credentials and encryption material, fetched with one
/// `PUT` and reused until they near `credExpiryTime`, the optional TTL passes, or S3
/// reports `ExpiredToken`.
const CREDS_REFRESH_MARGIN: Duration = Duration::from_secs(300);

struct StageCache {
    stage: String,
    ttl: Duration,
    cached: Mutex<Option<(Arc<StageInfo>, Instant)>>,
    fetches: AtomicUsize,
}

impl StageCache {
    async fn get(&self, session: &Session) -> Result<Arc<StageInfo>> {
        let mut guard = self.cached.lock().await;
        if let Some((info, at)) = guard.as_ref()
            && at.elapsed() < self.ttl
            && !info.creds_expiring(CREDS_REFRESH_MARGIN)
        {
            return Ok(info.clone());
        }
        let info = Arc::new(session.stage_info(&self.stage).await?);
        self.fetches.fetch_add(1, Ordering::Relaxed);
        *guard = Some((info.clone(), Instant::now()));
        Ok(info)
    }

    async fn invalidate(&self, seen: &Arc<StageInfo>) {
        let mut guard = self.cached.lock().await;
        if guard.as_ref().is_some_and(|(i, _)| Arc::ptr_eq(i, seen)) {
            *guard = None;
        }
    }
}

fn gzip_ndjson(run_id: &str, file_no: usize, rows: usize) -> Result<Vec<u8>> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    for i in 0..rows {
        let order_id = file_no * rows + i;
        let status = ["pending", "shipped", "delivered"][order_id % 3];
        let row = json!({
            "run_id": run_id,
            "file_no": file_no,
            "order_id": order_id,
            "customer_id": (order_id * 31) % 1000,
            "amount": format!("{}.{:02}", 10 + order_id % 90, order_id % 100),
            "status": status,
        });
        writeln!(enc, "{row}")?;
    }
    Ok(enc.finish()?)
}

struct Uploader<'a> {
    session: &'a Session,
    stages: &'a StageCache,
    s3: reqwest::Client,
    s3_counter: ConnCounter,
    fresh_client: bool,
}

impl Uploader<'_> {
    async fn upload(&self, key: &str, gz: &[u8]) -> Result<()> {
        let digest = B64.encode(Sha256::digest(gz));
        let client = if self.fresh_client {
            http_client(&self.s3_counter)?
        } else {
            self.s3.clone()
        };
        for _ in 0..2 {
            let stage = self.stages.get(self.session).await?;
            let (body, meta) = encrypt::encrypt(&stage.encryption, gz)?;
            match s3::put_object(&client, &stage, key, body, &digest, &meta).await? {
                PutOutcome::Uploaded => return Ok(()),
                PutOutcome::ExpiredToken => {
                    eprintln!("stage credentials expired; refreshing");
                    self.stages.invalidate(&stage).await;
                }
            }
        }
        bail!("upload of {key} failed after refreshing stage credentials")
    }

    async fn upload_batch(&self, prefix: &str, run_id: &str, args: &Args) -> Result<Vec<String>> {
        let keys: Vec<String> = (0..args.files)
            .map(|i| format!("{prefix}/{run_id}/part-{i:05}.json.gz"))
            .collect();
        let results: Vec<Result<()>> = stream::iter(keys.iter().enumerate())
            .map(|(i, key)| async move {
                let gz = gzip_ndjson(run_id, i, args.rows)?;
                self.upload(key, &gz).await
            })
            .buffer_unordered(args.concurrency)
            .collect()
            .await;
        for r in results {
            r?;
        }
        Ok(keys)
    }
}

async fn count_rows(session: &Session, table: &str, run_id: &str) -> Result<u64> {
    let r = session
        .query(&format!(
            "SELECT COUNT(*) FROM {table} WHERE v:run_id::string = '{run_id}'"
        ))
        .await?;
    let cell = r
        .rows
        .first()
        .and_then(|row| row.first())
        .context("empty COUNT result")?;
    Ok(match cell {
        Value::String(s) => s.parse()?,
        v => v.as_u64().context("COUNT not numeric")?,
    })
}

async fn run(args: Args) -> Result<()> {
    let account = env("SQLLOAD_SNOWFLAKE_ACCOUNT")?;
    let host = std::env::var("SQLLOAD_SNOWFLAKE_HOST")
        .unwrap_or_else(|_| format!("{}.snowflakecomputing.com", account.to_lowercase()));
    let cfg = Config {
        account,
        host,
        user: env("SQLLOAD_SNOWFLAKE_USER")?,
        pat: env("SQLLOAD_SNOWFLAKE_PAT")?,
        role: env("SQLLOAD_SNOWFLAKE_ROLE")?,
        warehouse: env("SQLLOAD_SNOWFLAKE_WAREHOUSE")?,
        database: env("SQLLOAD_SNOWFLAKE_DATABASE")?,
        schema: env("SQLLOAD_SNOWFLAKE_SCHEMA")?,
    };
    let qualify = |name: String| format!("{}.{}.{}", cfg.database, cfg.schema, name);
    let stage = qualify(env("SQLLOAD_STAGE")?);
    let table = qualify(env("SQLLOAD_TABLE")?);
    let pipe = std::env::var("SQLLOAD_PIPE").ok().map(qualify);
    let pipe_table = std::env::var("SQLLOAD_PIPE_TABLE").ok().map(qualify);

    let sf_counter = ConnCounter::default();
    let s3_counter = ConnCounter::default();
    let session = Session::new(http_client(&sf_counter)?, cfg);
    let stages = StageCache {
        stage: stage.clone(),
        ttl: args.stage_creds_ttl,
        cached: Mutex::new(None),
        fetches: AtomicUsize::new(0),
    };
    let uploader = Uploader {
        session: &session,
        stages: &stages,
        s3: http_client(&s3_counter)?,
        s3_counter: s3_counter.clone(),
        fresh_client: args.fresh_s3_client,
    };
    let run_id = hex::encode(util::random_bytes(6));
    let expected = (args.files * args.rows) as u64;
    let opens_before = tcp_active_opens();
    let started = Instant::now();

    session.login().await?;
    eprintln!("run {run_id}: {} files x {} rows", args.files, args.rows);

    let t = Instant::now();
    uploader.upload_batch("copy", &run_id, &args).await?;
    let upload_secs = t.elapsed().as_secs_f64();
    eprintln!(
        "uploaded {} files in {upload_secs:.2}s ({} stage-info PUT queries)",
        args.files,
        stages.fetches.load(Ordering::Relaxed)
    );

    if args.renew_session {
        session.renew(None).await?;
        eprintln!("session token renewed via /session/token-request");
    }

    if !args.skip_copy {
        let r = session
            .query(&format!(
                "COPY INTO {table} FROM @{stage}/copy/{run_id}/ FILE_FORMAT = (TYPE = JSON)"
            ))
            .await?;
        let status = r.column("status");
        let loaded_col = r.column("rows_loaded");
        let loaded_files = r
            .rows
            .iter()
            .filter(|row| status.and_then(|i| row[i].as_str()) == Some("LOADED"))
            .count();
        let rows_loaded: u64 = r
            .rows
            .iter()
            .filter_map(|row| loaded_col.and_then(|i| row[i].as_str()?.parse::<u64>().ok()))
            .sum();
        let counted = count_rows(&session, &table, &run_id).await?;
        eprintln!(
            "COPY: {loaded_files}/{} files LOADED, rows_loaded={rows_loaded}, table count={counted}, expected={expected}",
            args.files
        );
        if counted != expected || loaded_files != args.files {
            bail!("COPY verification failed");
        }
    }

    if let (Some(pipe), Some(pipe_table)) = (pipe, pipe_table) {
        let keys = uploader.upload_batch("pipe", &run_id, &args).await?;
        match session.insert_files(&pipe, &keys).await {
            Ok(resp) => {
                eprintln!(
                    "insertFiles accepted with PAT bearer: responseCode={}",
                    resp["responseCode"]
                );
                let deadline = Instant::now() + Duration::from_secs(180);
                loop {
                    let n = count_rows(&session, &pipe_table, &run_id).await?;
                    if n == expected {
                        eprintln!("Snowpipe loaded {n} rows");
                        break;
                    }
                    if Instant::now() > deadline {
                        bail!("Snowpipe loaded {n}/{expected} rows before timeout");
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            Err(e) => eprintln!("insertFiles not usable: {e:#}"),
        }
    }

    session.logout().await?;
    let opens_after = tcp_active_opens();
    println!(
        "{}",
        json!({
            "run_id": run_id,
            "files": args.files,
            "rows_per_file": args.rows,
            "concurrency": args.concurrency,
            "fresh_s3_client_per_file": args.fresh_s3_client,
            "upload_seconds": upload_secs,
            "total_seconds": started.elapsed().as_secs_f64(),
            "snowflake_connections": sf_counter.get(),
            "s3_connections": s3_counter.get(),
            "stage_info_fetches": stages.fetches.load(Ordering::Relaxed),
            "session_renewals": session.renewals.load(Ordering::Relaxed),
            "host_tcp_active_opens_delta": opens_before.zip(opens_after).map(|(b, a)| a - b),
        })
    );
    Ok(())
}

/// Writes an encrypted sample plus metadata for `tools/decrypt_with_connector.py`.
fn encrypt_fixture(dir: &str) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let material = encrypt::EncryptionMaterial {
        query_stage_master_key: B64.encode(util::random_bytes(32)),
        query_id: util::uuid_v4(),
        smk_id: 4242,
    };
    let plain = gzip_ndjson("fixture", 0, 1000)?;
    let (ct, meta) = encrypt::encrypt(&material, &plain)?;
    assert_eq!(encrypt::decrypt(&material, &meta, &ct)?, plain);
    std::fs::write(format!("{dir}/plain.bin"), &plain)?;
    std::fs::write(format!("{dir}/encrypted.bin"), &ct)?;
    std::fs::write(
        format!("{dir}/meta.json"),
        json!({
            "query_stage_master_key": material.query_stage_master_key,
            "query_id": material.query_id,
            "smk_id": material.smk_id,
            "key": meta.key,
            "iv": meta.iv,
            "matdesc": meta.matdesc,
        })
        .to_string(),
    )?;
    eprintln!("wrote fixture to {dir}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut argv = std::env::args().skip(1);
    match argv.next().as_deref() {
        Some("run") => run(parse_args(argv)?).await,
        Some("encrypt-fixture") => encrypt_fixture(&argv.next().context(USAGE)?),
        _ => {
            eprint!("{USAGE}");
            std::process::exit(2);
        }
    }
}
