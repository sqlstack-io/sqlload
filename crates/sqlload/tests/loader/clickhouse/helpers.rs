//! What the ClickHouse loader tests share: projects, Parquet in S3, and loads stopped at every statement.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use sqlload::adapters::clickhouse::main::session::stop_after::stop_after;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::write_file::write_file;
use sqlload::file_store::models::OpenFolder;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::PlanOptions;
use sqlload::project::models::ProjectConfig;
use sqlload::store_paths::models::{Location, StoreKind};
use sqlload_testkit::clickhouse::constants::{
    PASSWORD as CLICKHOUSE_PASSWORD, USER as CLICKHOUSE_USER,
};
use sqlload_testkit::seaweedfs::constants::{REGION, S3_PORT};
use sqlload_testkit::seaweedfs::models::SeaweedFs;

use crate::helpers::load_with_options;
use crate::test_types::{EventsFile, StoppedLoadTestCase};

/// A project reading the test Postgres into the test ClickHouse's `schema` database, with `tables` (source and table settings) appended.
pub(crate) fn clickhouse_project(
    postgres_port: u16,
    clickhouse_port: u16,
    schema: &str,
    tables: &str,
) -> String {
    format!(
        r#"
[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {postgres_port}
user = "sqlload"
password = "sqlload"
database = "app"

[connections.serving]
kind = "clickhouse"
host = "127.0.0.1"
port = {clickhouse_port}
user = "{CLICKHOUSE_USER}"
password = "{CLICKHOUSE_PASSWORD}"

[targets.dev]
connection = "serving"
loader_schema = "{schema}"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
{tables}
"#
    )
}

/// Plan, apply and load `options`' selection; the plan's text, or why it failed.
pub(crate) async fn plan_apply_load_with(
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

/// Options selecting only `table`.
pub(crate) fn select_only(table: &str) -> PlanOptions {
    PlanOptions {
        select: vec![table.to_owned()],
        ..PlanOptions::default()
    }
}

/// `query`'s single text on `serving`, empty when it returns none.
pub(crate) async fn read_text(serving: &Destination, query: &str) -> Result<String, String> {
    Ok(single_text(serving, query.to_owned())
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or_default())
}

/// How many stop points a stopped-load test tries; more than any of its loads' statements.
const STOP_POINTS: usize = 30;

/// Load `test_case.table` into the ClickHouse target `dev` (in `database`) once, then, for each N below `STOP_POINTS`, switch its source to the other state with `set_state` and load it with ClickHouse's statements for it stopped from the Nth on: after each stop the table must hold the old or the new state, and the rerun the new one. Returns the first N whose run was never stopped, once every statement ran.
pub(crate) async fn run_clickhouse_stops(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    set_state: &impl AsyncFn(&'static str) -> Result<(), String>,
    (database, test_case): (&str, &StoppedLoadTestCase),
) -> Result<Option<usize>, String> {
    let serving = destinations
        .get("dev")
        .ok_or_else(|| "no ClickHouse destination".to_owned())?;
    // The stop matches the table's qualified name, which its staging, delta and copy tables start with; a database of the test's own keeps it out of tests running at the same time.
    let stopped_name = format!("`{database}`.`{}", test_case.table);
    let [first, _] = test_case.states;
    set_state(first.0).await?;
    plan_apply_load_with(project, destinations, &select_only(test_case.table)).await?;
    assert_eq!(
        read_text(serving, test_case.query).await?,
        first.1,
        "{}: the first load",
        test_case.description
    );
    let mut unstopped = None;
    for stops in 0..STOP_POINTS {
        let (change, expected) = test_case.states[(stops + 1) % 2];
        let current = test_case.states[stops % 2].1;
        set_state(change).await?;
        stop_after(&stopped_name, Some(stops));
        let stopped = plan_apply_load_with(project, destinations, &test_case.options).await;
        stop_after(&stopped_name, None);
        let seen = read_text(serving, test_case.query).await?;
        assert!(
            seen == current || seen == expected,
            "{}: a run stopped after {stops} statements left the table half published: {seen}",
            test_case.description
        );
        assert!(
            stopped.is_err() || seen == expected,
            "{}: the run that was never stopped left {seen}",
            test_case.description
        );
        unstopped = unstopped.or(stopped.is_ok().then_some(stops));
        plan_apply_load_with(project, destinations, &test_case.options)
            .await
            .map_err(|error| format!("the rerun after {stops}: {error}"))?;
        assert_eq!(
            read_text(serving, test_case.query).await?,
            expected,
            "{}: the rerun after a stop at {stops} statements",
            test_case.description
        );
    }
    Ok(unstopped)
}

/// The bucket the ClickHouse pull tests read Parquet from.
pub(crate) const PULL_BUCKET: &str = "example-bucket-test";

/// Files sources in `endpoint`'s bucket under `prefix`, loaded into the test ClickHouse's `schema` database: daily event folders reread from a day before the newest, and times of day, which ClickHouse cannot read from Parquet exactly.
pub(crate) fn clickhouse_files_project(
    endpoint: &str,
    clickhouse_port: u16,
    (schema, prefix): (&str, &str),
) -> String {
    format!(
        r#"
[connections.lake]
kind = "s3"
bucket = "{PULL_BUCKET}"
region = "{REGION}"
endpoint = "{endpoint}"

[connections.serving]
kind = "clickhouse"
host = "127.0.0.1"
port = {clickhouse_port}
user = "{CLICKHOUSE_USER}"
password = "{CLICKHOUSE_PASSWORD}"

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

/// `batch` as Parquet in row groups of 700 rows.
fn row_grouped_parquet(batch: &RecordBatch) -> Result<bytes::Bytes, String> {
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

/// Parquet holding `ids`, a fixed-size `raw` value ending in zero bytes, and `note` when given.
pub(crate) fn events_parquet(
    ids: std::ops::RangeInclusive<i64>,
    note: Option<&str>,
) -> Result<bytes::Bytes, String> {
    let rows = ids.clone().count();
    let raw = arrow_array::FixedSizeBinaryArray::try_from_iter(ids.clone().map(|_| [0u8, 1, 0, 0]))
        .map_err(|error| error.to_string())?;
    let mut columns: Vec<(&str, arrow_array::ArrayRef)> = vec![
        (
            "id",
            std::sync::Arc::new(arrow_array::Int64Array::from_iter_values(ids)),
        ),
        ("raw", std::sync::Arc::new(raw)),
    ];
    columns.extend(note.map(|note| {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(arrow_array::StringArray::from(vec![note; rows]));
        ("note", array)
    }));
    row_grouped_parquet(&RecordBatch::try_from_iter(columns).map_err(|error| error.to_string())?)
}

/// Parquet holding one time of day, 13:04:05.123456.
pub(crate) fn clocks_parquet() -> Result<bytes::Bytes, String> {
    let times: arrow_array::ArrayRef =
        std::sync::Arc::new(arrow_array::Time64MicrosecondArray::from(vec![
            47_045_123_456_i64,
        ]));
    row_grouped_parquet(
        &RecordBatch::try_from_iter([("t", times)]).map_err(|error| error.to_string())?,
    )
}

/// The pull bucket's folder `prefix`, through `endpoint`.
pub(crate) fn pull_folder(endpoint: &str, prefix: &str) -> Result<OpenFolder, String> {
    open_folder(&Location {
        store: StoreKind::S3 {
            bucket: PULL_BUCKET.to_owned(),
            region: Some(REGION.to_owned()),
            endpoint: Some(endpoint.to_owned()),
        },
        prefix: format!("{prefix}/"),
    })
    .map_err(|error| error.to_string())
}

/// Write each of `files` into `folder`.
pub(crate) async fn put_events(folder: &OpenFolder, files: &[EventsFile]) -> Result<(), String> {
    for file in files {
        write_file(
            folder,
            file.key,
            events_parquet(file.ids.clone(), file.note)?,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Write one single-row `events/day=<day>/part.parquet` per `day=id` of the comma-separated `days`.
pub(crate) async fn put_day_parts(folder: &OpenFolder, days: &str) -> Result<(), String> {
    for part in days.split(',') {
        let (day, id) = part
            .split_once('=')
            .ok_or_else(|| format!("{part} is not day=id"))?;
        let id: i64 = id.parse().map_err(|error| format!("{part}: {error}"))?;
        write_file(
            folder,
            &format!("events/day={day}/part.parquet"),
            events_parquet(id..=id, None)?,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// SeaweedFS's S3 endpoint at its container's bridge address, which both this machine and the ClickHouse container reach.
pub(crate) async fn shared_s3_endpoint(seaweedfs: &SeaweedFs) -> Result<String, String> {
    let address = seaweedfs
        .container
        .get_bridge_ip_address()
        .await
        .map_err(|error| error.to_string())?;
    Ok(format!("http://{address}:{S3_PORT}"))
}
