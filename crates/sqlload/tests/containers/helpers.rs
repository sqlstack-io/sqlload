use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use serde_json::Value;
use sqlload::config::models::Config;
use sqlload::exporter::main::run_exporter::run_exporter;
use sqlload::exporter::models::RunSummary;
use sqlload_testkit::configs::main::streaming_config::streaming_config;
use sqlload_testkit::fake_snowflake::main::start_fake_snowflake::start_fake_snowflake;
use sqlload_testkit::fake_snowflake::models::FakeSnowflake;
use sqlload_testkit::redpanda::main::start_redpanda::start_redpanda;
use sqlload_testkit::redpanda::models::Redpanda;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) const WAIT: Duration = Duration::from_secs(90);
pub(crate) const PARTITIONS: i32 = 3;

/// A broker and a fake warehouse.
pub(crate) struct Stack {
    pub(crate) redpanda: Redpanda,
    pub(crate) fake: FakeSnowflake,
}

pub(crate) async fn start_stack() -> Result<Stack, String> {
    Ok(Stack {
        redpanda: start_redpanda().await?,
        fake: start_fake_snowflake().await?,
    })
}

/// Configuration for topics under `prefix`, plus `extra` TOML.
pub(crate) fn config(stack: &Stack, include: &str, extra: &str) -> Result<Config, String> {
    streaming_config(
        &stack.redpanda.brokers,
        &stack.fake.base_url,
        include,
        extra,
    )
}

/// `count` order records starting at `first`.
pub(crate) fn orders(first: usize, count: usize) -> Vec<String> {
    (first..first + count)
        .map(|index| {
            format!(
                r#"{{"order_id":{index},"customer":"customer-{}"}}"#,
                index % 11
            )
        })
        .collect()
}

/// A running exporter and the token that stops it.
pub(crate) struct Running {
    pub(crate) cancel: CancellationToken,
    pub(crate) task: JoinHandle<Result<RunSummary, String>>,
}

pub(crate) fn start_exporter(config: Config) -> Running {
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let task = tokio::spawn(async move { run_exporter(config, None, token).await });
    Running { cancel, task }
}

/// Stop an exporter gracefully and return its summary.
pub(crate) async fn stop(running: Running) -> Result<RunSummary, String> {
    running.cancel.cancel();
    running.task.await.map_err(|error| error.to_string())?
}

/// Wait until `table` holds at least `rows` rows.
pub(crate) async fn wait_for_rows(
    fake: &FakeSnowflake,
    table: &str,
    rows: usize,
) -> Result<(), String> {
    let started = Instant::now();
    while fake.rows(table).len() < rows {
        if started.elapsed() > WAIT {
            return Err(format!(
                "{table} has {} of {rows} rows",
                fake.rows(table).len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

/// Wait until the fake has accepted at least `appends` append requests.
pub(crate) async fn wait_for_appends(fake: &FakeSnowflake, appends: u64) -> Result<(), String> {
    let started = Instant::now();
    while fake.appends() < appends {
        if started.elapsed() > WAIT {
            return Err(format!("only {} appends", fake.appends()));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Distinct (topic, partition, offset) triples in `rows`.
pub(crate) fn distinct_positions(rows: &[Value]) -> usize {
    let positions: BTreeSet<(String, i64, i64)> = rows
        .iter()
        .map(|row| {
            (
                row["TOPIC"].as_str().unwrap_or_default().to_owned(),
                row["KAFKA_PARTITION"].as_i64().unwrap_or(-1),
                row["KAFKA_OFFSET"].as_i64().unwrap_or(-1),
            )
        })
        .collect();
    positions.len()
}

/// `count` JSON strings of `length` letters gzip cannot shrink much.
pub(crate) fn incompressible(count: usize, length: usize) -> Vec<String> {
    (0..count)
        .map(|seed| format!("\"{}\"", noise(length, seed as u64 + 1)))
        .collect()
}

fn noise(length: usize, seed: u64) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}
