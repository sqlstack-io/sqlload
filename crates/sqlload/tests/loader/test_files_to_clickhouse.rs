use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use sqlload::adapters::clickhouse::main::session::stop_after::stop_after;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::write_file::write_file;
use sqlload::file_store::models::OpenFolder;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::PlanOptions;
use sqlload::project::models::ProjectConfig;
use sqlload::store_paths::models::{Location, StoreKind};
use sqlload_testkit::clickhouse::constants::{PASSWORD, USER};
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::seaweedfs::constants::{REGION, S3_PORT};
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{described_project, load_with_options, run_async};

const BUCKET: &str = "example-bucket-test";

/// Files sources in `endpoint`'s bucket under `prefix`, loaded into the test ClickHouse's `schema` database: daily event folders reread from a day before the newest, and times of day, which ClickHouse cannot read from Parquet exactly.
fn project(endpoint: &str, clickhouse_port: u16, (schema, prefix): (&str, &str)) -> String {
    format!(
        r#"
[connections.lake]
kind = "s3"
bucket = "{BUCKET}"
region = "{REGION}"
endpoint = "{endpoint}"

[connections.serving]
kind = "clickhouse"
host = "127.0.0.1"
port = {clickhouse_port}
user = "{USER}"
password = "{PASSWORD}"

[targets.dev]
connection = "serving"
loader_schema = "{schema}"

[sources.events]
kind = "files"
description = "Events in daily folders."
connection = "lake"
path = "{prefix}/events/"
write_strategy = "delete_insert"
cursor_column = "day"
lookback = "1d"
partition_columns = {{ day = "date" }}
partition_by = "toYYYYMM(day)"

[sources.clocks]
kind = "files"
description = "Times of day."
connection = "lake"
path = "{prefix}/clocks/"
write_strategy = "replace"
"#
    )
}

/// Parquet holding `ids`, a fixed-size `raw` value ending in zero bytes, and `note` when given, in row groups of 700 rows.
fn events_file(ids: &[i64], note: Option<&str>) -> Result<bytes::Bytes, String> {
    let raw = FixedSizeBinaryArray::try_from_iter(ids.iter().map(|_| [0u8, 1, 0, 0]))
        .map_err(|error| error.to_string())?;
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(Int64Array::from(ids.to_vec()))),
        ("raw", Arc::new(raw)),
    ];
    if let Some(note) = note {
        columns.push(("note", Arc::new(StringArray::from(vec![note; ids.len()]))));
    }
    parquet_bytes(&RecordBatch::try_from_iter(columns).map_err(|error| error.to_string())?)
}

fn parquet_bytes(batch: &RecordBatch) -> Result<bytes::Bytes, String> {
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(700))
        .build();
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties))
            .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(bytes::Bytes::from(buffer))
}

async fn put(folder: &OpenFolder, key: &str, body: bytes::Bytes) -> Result<(), String> {
    write_file(folder, key, body)
        .await
        .map_err(|error| error.to_string())
}

/// The bucket's folder `prefix`, through `endpoint`.
fn lake(endpoint: &str, prefix: &str) -> Result<OpenFolder, String> {
    open_folder(&Location {
        store: StoreKind::S3 {
            bucket: BUCKET.to_owned(),
            region: Some(REGION.to_owned()),
            endpoint: Some(endpoint.to_owned()),
        },
        prefix: format!("{prefix}/"),
    })
    .map_err(|error| error.to_string())
}

/// Plan, apply and load `options`' selection; the plan's text, and the load's error, if any.
async fn load(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<String, String> {
    let plan = plan_project(project, destinations, options)
        .await
        .map_err(|error| error.to_string())?;
    let (lines, ok) = render_plan(&plan);
    let text = lines.join("\n");
    ok.then_some(()).ok_or_else(|| text.clone())?;
    apply_project(&plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    load_with_options(project, &plan, destinations, options).await?;
    Ok(text)
}

async fn read(serving: &Destination, query: &str) -> Result<String, String> {
    Ok(single_text(serving, query.to_owned())
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or_default())
}

/// SeaweedFS's S3 endpoint at its container's bridge address, which both this machine and the ClickHouse container reach.
async fn shared_endpoint(
    seaweedfs: &sqlload_testkit::seaweedfs::models::SeaweedFs,
) -> Result<String, String> {
    let address = seaweedfs
        .container
        .get_bridge_ip_address()
        .await
        .map_err(|error| error.to_string())?;
    Ok(format!("http://{address}:{S3_PORT}"))
}

#[test]
fn given_parquet_in_s3_when_loading_into_clickhouse_then_clickhouse_reads_the_files_itself_exactly()
-> Result<(), String> {
    run_async(async {
        let (clickhouse, seaweedfs) = tokio::try_join!(start_clickhouse(), start_seaweedfs())?;
        create_bucket(&seaweedfs, BUCKET).await?;
        let endpoint = shared_endpoint(&seaweedfs).await?;
        let folder = lake(&endpoint, "pulls")?;
        let first: Vec<i64> = (1..=5000).collect();
        put(
            &folder,
            "events/day=2026-10-30/part one.parquet",
            events_file(&first, Some("a"))?,
        )
        .await?;
        put(
            &folder,
            "events/day=2026-11-01/b.parquet",
            events_file(&[9001, 9002], None)?,
        )
        .await?;
        let times = arrow_array::Time64MicrosecondArray::from(vec![47_045_123_456_i64]);
        put(
            &folder,
            "clocks/c.parquet",
            parquet_bytes(
                &RecordBatch::try_from_iter([("t", Arc::new(times) as ArrayRef)])
                    .map_err(|error| error.to_string())?,
            )?,
        )
        .await?;
        let project =
            described_project(&project(&endpoint, clickhouse.port, ("raw", "pulls"))).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(serving) = destinations.get("dev") else {
            return Err("no ClickHouse destination".to_owned());
        };
        let plan = load(&project, &destinations, &PlanOptions::default()).await?;
        assert!(
            plan.contains(&format!(
                "route   ClickHouse reads directly from s3://{BUCKET}/pulls/events/ (nothing through the loader)"
            )),
            "Parquet in S3 is read by ClickHouse itself: {plan}"
        );
        assert!(
            plan.contains("route   read parquet into Arrow -> ClickHouse takes Arrow"),
            "a time column, which ClickHouse reads as a timestamp, goes through SQLLoad: {plan}"
        );
        let events = "SELECT toString(count()) || '|' || toString(countIf(_sqlload_source_row != id - if(id > 9000, 9000, 0))) || '|' || arrayStringConcat(groupUniqArray(hex(raw)), ',') || '|' || toString(countIf(note IS NULL)) || '|' || arrayStringConcat(arraySort(groupUniqArray(toString(day) || '@' || _sqlload_source_file)), ',') FROM raw.events";
        let checks = [
            (
                "each file's rows numbered in stored order across row groups, fixed-size values kept whole, a column a file lacks NULL, days from the folders and keys with spaces read",
                events,
                "5002|0|00010000|2|2026-10-30@s3://example-bucket-test/pulls/events/day=2026-10-30/part one.parquet,2026-11-01@s3://example-bucket-test/pulls/events/day=2026-11-01/b.parquet",
            ),
            (
                "the time column loaded through SQLLoad as its text",
                "SELECT arrayStringConcat(groupArray(t), ',') FROM raw.clocks",
                "13:04:05.123456",
            ),
        ];
        for (description, query, expected) in checks {
            assert_eq!(read(serving, query).await?, expected, "{description}");
        }
        put(
            &folder,
            "events/day=2026-11-01/c.parquet",
            events_file(&[9003], Some("c"))?,
        )
        .await?;
        load(&project, &destinations, &PlanOptions::default()).await?;
        assert_eq!(
            read(
                serving,
                "SELECT toString(count()) || '|' || arrayStringConcat(arraySort(groupArray(toString(id))), ',') FROM raw.events WHERE day = '2026-11-01'"
            )
            .await?,
            "3|9001,9002,9003",
            "a new file in a folder replaces that day's rows with every file's, once"
        );
        assert_eq!(
            read(serving, "SELECT toString(count()) FROM raw.events").await?,
            "5003",
            "the other day's rows are kept"
        );
        Ok(())
    })
}

#[test]
fn given_a_pull_stopped_at_every_statement_when_it_runs_again_then_the_table_was_never_half_published_and_converges()
-> Result<(), String> {
    run_async(async {
        let (clickhouse, seaweedfs) = tokio::try_join!(start_clickhouse(), start_seaweedfs())?;
        create_bucket(&seaweedfs, BUCKET).await?;
        let endpoint = shared_endpoint(&seaweedfs).await?;
        let folder = lake(&endpoint, "stops")?;
        // The window rereads from the day before the newest, so each run rebuilds October (keeping the 30th's rows) and November at once.
        put(
            &folder,
            "events/day=2026-10-30/kept.parquet",
            events_file(&[50], Some("kept"))?,
        )
        .await?;
        let states: [(&[(&str, i64)], &str); 2] = [
            (&[("2026-10-31", 100), ("2026-11-01", 200)], "100,200,50"),
            (&[("2026-10-31", 101), ("2026-11-01", 201)], "101,201,50"),
        ];
        let write_state = |state: &'static [(&'static str, i64)]| {
            let folder = &folder;
            async move {
                for (day, id) in state {
                    put(
                        folder,
                        &format!("events/day={day}/part.parquet"),
                        events_file(&[*id], None)?,
                    )
                    .await?;
                }
                Ok::<(), String>(())
            }
        };
        write_state(states[0].0).await?;
        let project =
            described_project(&project(&endpoint, clickhouse.port, ("stops", "stops"))).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(serving) = destinations.get("dev") else {
            return Err("no ClickHouse destination".to_owned());
        };
        let only = PlanOptions {
            select: vec!["events".to_owned()],
            ..PlanOptions::default()
        };
        load(&project, &destinations, &only).await?;
        let ids =
            "SELECT arrayStringConcat(arraySort(groupArray(toString(id))), ',') FROM stops.events";
        assert_eq!(read(serving, ids).await?, states[0].1, "the first load");
        let mut stops: usize = 0;
        loop {
            let (change, expected) = states[(stops + 1) % 2];
            let current = states[stops % 2].1;
            write_state(change).await?;
            stop_after("`stops`.`events", Some(stops));
            let stopped = load(&project, &destinations, &only).await;
            stop_after("`stops`.`events", None);
            let seen = read(serving, ids).await?;
            assert!(
                seen == current || seen == expected,
                "a pull stopped after {stops} statements left the table half published: {seen}"
            );
            if stopped.is_ok() {
                assert_eq!(seen, expected, "the run that was never stopped");
                break;
            }
            load(&project, &destinations, &only)
                .await
                .map_err(|error| format!("the rerun after {stops}: {error}"))?;
            assert_eq!(
                read(serving, ids).await?,
                expected,
                "the rerun after a stop at {stops} statements"
            );
            stops += 1;
        }
        assert!(stops >= 8, "only {stops} stop points were tried");
        Ok(())
    })
}
