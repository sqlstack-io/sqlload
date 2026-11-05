use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use sqlload::adapters::dispatch::main::session::execute::execute;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::files_source::main::discover_files::discover_files;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{LoadRun, PlanOptions, ProjectPlan};
use sqlload::project::main::parse_project::parse_project;
use sqlload::project::models::ProjectConfig;
use sqlload::store_paths::models::{Location, StoreKind};
use sqlload_testkit::seaweedfs::constants::REGION;
use tokio_postgres::Client;

use crate::test_types::{
    BatchedScenarioTestCase, DebugStageTestCase, FilesScenario, FilesScenarioTestCase,
    FilesTargetScenarioTestCase, LargePartitionTestCase, MetadataRoundTripTestCase, ParquetFile,
    RouteFiles, RouteOutcome, RouteTestCase, ScenarioTestCase, SourceRowUpgradeTestCase,
    TextFilesScenarioTestCase, WrittenPartition, WrittenRow, WrittenRun,
};

/// Run an async test body on a fresh multi-threaded runtime.
pub(crate) fn run_async<F: std::future::Future<Output = Result<(), String>>>(
    body: F,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(body)
}

/// Every value of `column` across `batches`, as Arrow displays it.
pub(crate) fn column_values(batches: &[RecordBatch], column: &str) -> Result<Vec<String>, String> {
    let options = FormatOptions::default();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .ok_or_else(|| format!("no column {column}"))?;
        let formatter =
            ArrayFormatter::try_new(array.as_ref(), &options).map_err(|error| error.to_string())?;
        values.extend((0..array.len()).map(|row| formatter.value(row).to_string()));
    }
    Ok(values)
}

/// A loader project reading `public.*` from the test Postgres into an in-memory DuckDB, plus `extra` TOML.
pub(crate) fn duckdb_project(port: u16, extra: &str) -> String {
    format!(
        r#"
[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {port}
user = "sqlload"
password = "sqlload"
database = "app"

[connections.local]
kind = "duckdb"
database = ":memory:"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
exclude_columns = ["*email*"]
{extra}
"#
    )
}

/// Plan, apply and load once; the error text when any step fails.
async fn run_once(
    project_text: &str,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<(), String> {
    let project = parse_project(project_text, &|_| None).map_err(|error| error.to_string())?;
    let plan = plan_project(&project, destinations, options)
        .await
        .map_err(|error| error.to_string())?;
    let (lines, ok) = render_plan(&plan);
    ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
    apply_project(&plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    load_all(&project, &plan, destinations).await
}

/// Load every table of `plan`; the first table that failed is the error.
pub(crate) async fn load_all(
    project: &ProjectConfig,
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
) -> Result<(), String> {
    load_with(project, plan, destinations, &PlanOptions::default()).await
}

/// Load every table of `plan` with `options`; every failed table is in the error.
async fn load_with(
    project: &ProjectConfig,
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<(), String> {
    let run = LoadRun {
        project,
        destinations,
        host: None,
        options,
    };
    let failures: Vec<String> = load_tables(&run, plan)
        .await
        .into_iter()
        .filter_map(|outcome| {
            outcome
                .loaded
                .err()
                .map(|reason| format!("{}: {reason}", outcome.table))
        })
        .collect();
    failures
        .is_empty()
        .then_some(())
        .ok_or_else(|| failures.join("\n"))
}

/// Run a batched scenario on fresh source tables and a fresh DuckDB; returns each run's outcome as `<load error, or ok> => <check query result>`, the error cut to its first two parts (`table: what failed`), which stay the same from run to run.
pub(crate) async fn run_batched(
    client: &Client,
    port: u16,
    test_case: &BatchedScenarioTestCase,
) -> Result<Vec<String>, String> {
    client
        .batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let text = duckdb_project(port, test_case.settings);
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    let mut outcomes: Vec<String> = Vec::new();
    for run in test_case.runs {
        client
            .batch_execute(run.source_change)
            .await
            .map_err(|error| error.to_string())?;
        let hand_edits: Vec<String> = run
            .destination_change
            .split_terminator(';')
            .map(str::to_owned)
            .collect();
        execute(duckdb, hand_edits)
            .await
            .map_err(|error| error.to_string())?;
        let options = PlanOptions {
            end_cursor: run.end_cursor.map(str::to_owned),
            batch_bytes: test_case.batch_bytes,
            concurrency: test_case.concurrency,
            ..PlanOptions::default()
        };
        let plan = plan_project(&project, &destinations, &options)
            .await
            .map_err(|error| error.to_string())?;
        let (lines, ok) = render_plan(&plan);
        ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
        apply_project(&plan, &destinations)
            .await
            .map_err(|error| error.to_string())?;
        let error = load_with(&project, &plan, &destinations, &options)
            .await
            .err()
            .map_or_else(
                || "ok".to_owned(),
                |error| error.splitn(3, ": ").take(2).collect::<Vec<_>>().join(": "),
            );
        let result = single_text(duckdb, run.check_query.to_owned())
            .await
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        let shown = lines
            .iter()
            .filter(|_| !run.show.is_empty())
            .filter_map(|line| line.split_once(run.show))
            .map(|(_, rest)| format!(" | {}{rest}", run.show))
            .collect::<String>();
        outcomes.push(format!("{error} => {result}{shown}"));
    }
    Ok(outcomes)
}

/// The rendered plan, or the error that stopped it.
async fn plan_text(
    project_text: &str,
    destinations: &BTreeMap<String, Destination>,
) -> Result<String, String> {
    let project = parse_project(project_text, &|_| None).map_err(|error| error.to_string())?;
    let plan = plan_project(&project, destinations, &PlanOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    Ok(render_plan(&plan).0.join("\n"))
}

/// Run a scenario's loads on fresh source tables and a fresh DuckDB; the check query's result.
pub(crate) async fn run_scenario(
    client: &Client,
    port: u16,
    test_case: &ScenarioTestCase,
) -> Result<Option<String>, String> {
    client
        .batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let text = duckdb_project(port, test_case.settings);
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    for (index, run) in test_case.runs.iter().enumerate() {
        client
            .batch_execute(run.source_change)
            .await
            .map_err(|error| error.to_string())?;
        let hand_edits: Vec<String> = run
            .destination_change
            .split_terminator(';')
            .map(str::to_owned)
            .collect();
        execute(duckdb, hand_edits)
            .await
            .map_err(|error| error.to_string())?;
        let options = PlanOptions {
            reload: run.reload,
            start_cursor: run.window.0.map(str::to_owned),
            end_cursor: run.window.1.map(str::to_owned),
            ..PlanOptions::default()
        };
        let error = run_once(&text, &destinations, &options)
            .await
            .err()
            .unwrap_or_default();
        assert!(
            error.contains(run.expected_error) && error.is_empty() == run.expected_error.is_empty(),
            "{} run {}: {error}",
            test_case.description,
            index + 1
        );
    }
    single_text(duckdb, test_case.check_query.to_owned())
        .await
        .map_err(|error| error.to_string())
}

/// A local directory store, and its `[connections.files]` TOML.
pub(crate) fn local_store(_: &str, directory: &std::path::Path) -> (StoreKind, String) {
    let root = directory.display().to_string();
    let toml = format!("kind = \"filesystem\"\nroot = \"{root}\"");
    (StoreKind::Filesystem { root }, toml)
}

/// The SeaweedFS bucket store, and its `[connections.files]` TOML.
pub(crate) fn seaweedfs_store(endpoint: &str, _: &std::path::Path) -> (StoreKind, String) {
    let toml = format!(
        "kind = \"s3\"\nbucket = \"example-bucket-test\"\nregion = \"{REGION}\"\nendpoint = \"{endpoint}\""
    );
    let store = StoreKind::S3 {
        bucket: "example-bucket-test".to_owned(),
        region: Some(REGION.to_owned()),
        endpoint: Some(endpoint.to_owned()),
    };
    (store, toml)
}

/// A Parquet file of `file`'s rows.
fn parquet_bytes(file: &ParquetFile) -> Result<bytes::Bytes, String> {
    let mut columns: Vec<(&str, arrow_array::ArrayRef)> = vec![
        (
            "id",
            std::sync::Arc::new(arrow_array::Int64Array::from(file.ids.to_vec())),
        ),
        (
            "score",
            std::sync::Arc::new(arrow_array::Float64Array::from(vec![
                file.score;
                file.ids.len()
            ])),
        ),
    ];
    columns.extend(file.extra.map(|extra| {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(arrow_array::StringArray::from(vec![extra; file.ids.len()]));
        ("extra", array)
    }));
    let batch = RecordBatch::try_from_iter(columns).map_err(|error| error.to_string())?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(bytes::Bytes::from(buffer))
}

/// A `delete_insert` files source into DuckDB with one partition of `test_case.files` files that alone have an `extra` column (outside the newest files the plan samples), then that partition's neighbour rewritten; the check query's result.
pub(crate) async fn run_large_partition(
    directory: &std::path::Path,
    test_case: &LargePartitionTestCase,
) -> Result<Option<String>, String> {
    let wide: Vec<(String, ParquetFile)> = (1..=test_case.files)
        .map(|id| {
            (
                format!("day=2026-11-01/part-{id:05}.parquet"),
                ParquetFile {
                    key: "",
                    ids: &[],
                    score: id as f64,
                    extra: Some("x"),
                },
            )
        })
        .collect();
    let mut first: Vec<(&str, bytes::Bytes)> = Vec::new();
    for (key, file) in &wide {
        first.push((
            key.as_str(),
            parquet_bytes(&ParquetFile { ids: &[7], ..*file })?,
        ));
    }
    let late = ParquetFile {
        key: "day=2026-11-02/part-0.parquet",
        ids: &[8],
        score: 0.0,
        extra: None,
    };
    first.push((late.key, parquet_bytes(&late)?));
    let rewritten = ParquetFile {
        ids: &[8, 9],
        ..late
    };
    let scenario = FilesScenario {
        description: test_case.description,
        store: local_store,
        settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\npartition_columns = { day = \"date\" }\nbatch_size = \"1d\"\n",
        runs: vec![
            WrittenRun {
                files: first,
                expected_error: "",
                expected_plan: "2 batches of whole partitions, up to 1,000 files or about 1 GB per commit; each batch's footers are combined",
            },
            WrittenRun {
                files: vec![(rewritten.key, parquet_bytes(&rewritten)?)],
                expected_error: "",
                expected_plan: test_case.expected_plan,
            },
        ],
        check_query: "SELECT count(*) || ',' || count(extra) || ',' || count(DISTINCT _sqlload_source_file) || ',' || string_agg(DISTINCT CAST(id AS VARCHAR), '/' ORDER BY CAST(id AS VARCHAR)) FROM raw.scores",
    };
    run_written_files("", directory, scenario).await
}

/// Run a files scenario: write each run's files under a fresh `scores-<uuid>/` folder, then plan, apply and load into a fresh DuckDB.
pub(crate) async fn run_files_scenario(
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &FilesScenarioTestCase,
) -> Result<Option<String>, String> {
    let runs: Vec<WrittenRun<'_>> = test_case
        .runs
        .iter()
        .map(|run| {
            let files = run
                .files
                .iter()
                .map(|file| Ok((file.key, parquet_bytes(file)?)))
                .collect::<Result<Vec<_>, String>>()?;
            Ok(WrittenRun {
                files,
                expected_error: run.expected_error,
                expected_plan: "",
            })
        })
        .collect::<Result<_, String>>()?;
    let scenario = FilesScenario {
        description: test_case.description,
        store: test_case.store,
        settings: test_case.settings,
        runs,
        check_query: test_case.check_query,
    };
    run_written_files(endpoint, directory, scenario).await
}

/// Run a JSON or CSV files scenario like [`run_files_scenario`].
pub(crate) async fn run_text_files_scenario(
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &TextFilesScenarioTestCase,
) -> Result<Option<String>, String> {
    let runs: Vec<WrittenRun<'_>> = test_case
        .runs
        .iter()
        .map(|run| {
            let files = run
                .files
                .iter()
                .map(|file| Ok((file.key, (file.encode)(file.contents)?)))
                .collect::<Result<Vec<_>, String>>()?;
            Ok(WrittenRun {
                files,
                expected_error: run.expected_error,
                expected_plan: run.expected_plan,
            })
        })
        .collect::<Result<_, String>>()?;
    let scenario = FilesScenario {
        description: test_case.description,
        store: test_case.store,
        settings: test_case.settings,
        runs,
        check_query: test_case.check_query,
    };
    run_written_files(endpoint, directory, scenario).await
}

/// A file as written, uncompressed.
pub(crate) fn plain(text: &str) -> Result<bytes::Bytes, String> {
    Ok(bytes::Bytes::from(text.to_owned()))
}

/// A file gzip-compressed, in two gzip members (as concatenated uploads write them).
pub(crate) fn gzip(text: &str) -> Result<bytes::Bytes, String> {
    use std::io::Write;
    let (first, second) = text.as_bytes().split_at(text.len() / 2);
    let mut compressed: Vec<u8> = Vec::new();
    for part in [first, second] {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(part).map_err(|error| error.to_string())?;
        compressed.extend(encoder.finish().map_err(|error| error.to_string())?);
    }
    Ok(bytes::Bytes::from(compressed))
}

/// A gzip file whose first 10,000 lines (`{"id":0}`, ...) fill the schema sample, so the text comes after it.
pub(crate) fn after_sample(text: &str) -> Result<bytes::Bytes, String> {
    let padding: String = (0..10_000).map(|id| format!("{{\"id\":{id}}}\n")).collect();
    gzip(&format!("{padding}{text}"))
}

/// A file zstd-compressed.
pub(crate) fn zstd(text: &str) -> Result<bytes::Bytes, String> {
    zstd::encode_all(text.as_bytes(), 3)
        .map(bytes::Bytes::from)
        .map_err(|error| error.to_string())
}

async fn run_written_files(
    endpoint: &str,
    directory: &std::path::Path,
    scenario: FilesScenario<'_>,
) -> Result<Option<String>, String> {
    let (store, connection) = (scenario.store)(endpoint, directory);
    let prefix = format!("scores-{}/", uuid::Uuid::new_v4());
    let text = format!(
        "[connections.files]\n{}\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.scores]\nkind = \"files\"\nconnection = \"files\"\npath = \"{prefix}\"\n{}\n",
        connection, scenario.settings
    );
    let folder = open_folder(&Location { store, prefix }).map_err(|error| error.to_string())?;
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    for (index, run) in scenario.runs.into_iter().enumerate() {
        let (expected_error, expected_plan) = (run.expected_error, run.expected_plan);
        for (key, contents) in run.files {
            write_file(&folder, key, contents)
                .await
                .map_err(|error| error.to_string())?;
        }
        let plan = plan_text(&text, &destinations)
            .await
            .unwrap_or_else(|error| error);
        assert!(
            plan.contains(expected_plan),
            "{} run {}: {plan}",
            scenario.description,
            index + 1
        );
        let error = run_once(&text, &destinations, &PlanOptions::default())
            .await
            .err()
            .unwrap_or_default();
        assert!(
            error.contains(expected_error) && error.is_empty() == expected_error.is_empty(),
            "{} run {}: {error}",
            scenario.description,
            index + 1
        );
    }
    single_text(duckdb, scenario.check_query.to_owned())
        .await
        .map_err(|error| error.to_string())
}

/// A project loading every `orders` table from the test Postgres into the files connection `lake`, plus `target` under `[targets.lake]` and `settings` under `[sources.app]`.
fn files_target_project(port: u16, connection: &str, target: &str, settings: &str) -> String {
    format!(
        r#"
[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {port}
user = "sqlload"
password = "sqlload"
database = "app"

[connections.lake]
{connection}

[targets.lake]
connection = "lake"
path = "PREFIX/{{source}}/{{table}}/"
{target}

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["*.orders"]
{settings}
"#
    )
}

pub(crate) fn two_days() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files = day_files("2026-11-01", (2, 0), None)?;
    files.extend(day_files("2026-11-02", (1, 0), None)?);
    Ok(files)
}

/// Enough files for two batches.
pub(crate) fn two_batches() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files = day_files("2026-11-01", (1000, 0), None)?;
    files.extend(day_files("2026-11-02", (1, 0), None)?);
    Ok(files)
}

pub(crate) fn with_text() -> Result<Vec<(String, bytes::Bytes)>, String> {
    day_files("2026-11-01", (1, 0), Some("{\"a\": 1}"))
}

pub(crate) fn long_names() -> Result<Vec<(String, bytes::Bytes)>, String> {
    day_files("2026-11-01", (300, 1500), None)
}

/// A full first batch of plain files, then a batch holding one the direct `COPY` must not load (`Float16`); the newest files, which planning samples, are plain.
pub(crate) fn half_float_later() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files = day_files("2026-11-01", (1000, 0), None)?;
    files.push(half_float_file("day=2026-11-02/part-0.parquet")?);
    files.extend(day_files("2026-11-03", (25, 0), None)?);
    Ok(files)
}

pub(crate) fn nothing(_: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {}

pub(crate) fn no_user_stage(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    fake.state.lock().denied_stages.push("@~".to_owned());
}

pub(crate) fn no_internal_stage(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    fake.state
        .lock()
        .denied_stages
        .extend(["@~".to_owned(), "%".to_owned()]);
}

pub(crate) fn no_stage_or_host(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    no_internal_stage(fake);
    fake.state.lock().streaming_unreachable = true;
}

pub(crate) fn no_stage_or_channel(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    no_internal_stage(fake);
    fake.state.lock().channels_refused = true;
}

pub(crate) fn integration(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    fake.state.lock().integrations.insert("LAKE_INT".to_owned());
}

/// The integration, and a load folder a failed run left long ago.
pub(crate) fn old_leftovers(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    integration(fake);
    let mut state = fake.state.lock();
    state.objects_modified = Some("2020-01-01T00:00:00.000Z".to_owned());
    state.objects.insert(
        "fake-bucket/_sqlload/old-load/00001/part-00001.parquet".to_owned(),
        b"PAR1".to_vec(),
    );
}

/// The managed stage already exists, reading the bucket.
pub(crate) fn stage_exists(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    fake.state.lock().stages.insert(
        "_SQLLOAD_STAGE".to_owned(),
        ("s3://fake-bucket/".to_owned(), "LAKE_INT".to_owned()),
    );
}

/// Only an integration `LAKE_INT` matches through `SHOW`'s wildcard exists.
pub(crate) fn similar_integration(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    fake.state.lock().integrations.insert("LAKEXINT".to_owned());
    fake.state
        .lock()
        .integrations
        .insert("LAKE_INT_OLD".to_owned());
}

/// The driver answers every `PUT` with a server problem.
pub(crate) fn flaky_probes(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    fake.state.lock().driver_failure = Some(("000603".to_owned(), "internal error".to_owned()));
}

/// Files whose names pass the statement limit in total, with a text column to hash.
pub(crate) fn long_names_with_text() -> Result<Vec<(String, bytes::Bytes)>, String> {
    day_files("2026-11-01", (300, 1500), Some("x"))
}

/// One Parquet file with a timestamp column of `nanos` nanoseconds or microseconds.
fn timestamp_file(key: String, nanos: bool) -> Result<(String, bytes::Bytes), String> {
    let units: [arrow_array::ArrayRef; 2] = [
        std::sync::Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![1])),
        std::sync::Arc::new(arrow_array::TimestampNanosecondArray::from(vec![1_000])),
    ];
    let at = units[usize::from(nanos)].clone();
    let ids: arrow_array::ArrayRef = std::sync::Arc::new(arrow_array::Int64Array::from(vec![1]));
    let batch =
        RecordBatch::try_from_iter([("id", ids), ("at", at)]).map_err(|error| error.to_string())?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok((key, bytes::Bytes::from(buffer)))
}

/// The newest files, which planning samples, hold nanoseconds.
pub(crate) fn nanos_newest() -> Result<Vec<(String, bytes::Bytes)>, String> {
    Ok(vec![timestamp_file(
        "day=2026-11-01/part-0.parquet".to_owned(),
        true,
    )?])
}

/// A full batch of microseconds, a batch of nanoseconds, then a batch of microseconds, the newest files planning samples.
pub(crate) fn nanos_in_a_later_batch() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files: Vec<(String, bytes::Bytes)> = Vec::new();
    for (day, count, nanos) in [
        ("2026-11-01", 1000, false),
        ("2026-11-02", 1000, true),
        ("2026-11-03", 25, false),
    ] {
        for index in 0..count {
            files.push(timestamp_file(
                format!("day={day}/part-{index:05}.parquet"),
                nanos,
            )?);
        }
    }
    Ok(files)
}

/// One Parquet file whose timestamp is an INT96 under an embedded Arrow type of microseconds, which `ArrowWriter` cannot produce.
fn int96_file(key: String) -> Result<(String, bytes::Bytes), String> {
    let text = |error: parquet::errors::ParquetError| error.to_string();
    let schema = parquet::schema::parser::parse_message_type(
        "message m { required int64 id; required int96 at; }",
    )
    .map_err(text)?;
    let arrow = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new(
            "at",
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        ),
    ]);
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
            parquet::arrow::ARROW_SCHEMA_META_KEY.to_owned(),
            parquet::arrow::encode_arrow_schema(&arrow),
        )]))
        .build();
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = parquet::file::writer::SerializedFileWriter::new(
        &mut buffer,
        std::sync::Arc::new(schema),
        std::sync::Arc::new(properties),
    )
    .map_err(text)?;
    let mut group = writer.next_row_group().map_err(text)?;
    let mut ids = group.next_column().map_err(text)?.ok_or("no id column")?;
    ids.typed::<parquet::data_type::Int64Type>()
        .write_batch(&[1], None, None)
        .map_err(text)?;
    ids.close().map_err(text)?;
    let mut ats = group.next_column().map_err(text)?.ok_or("no at column")?;
    ats.typed::<parquet::data_type::Int96Type>()
        .write_batch(
            &[parquet::data_type::Int96::from(vec![0, 0, 2_440_588])],
            None,
            None,
        )
        .map_err(text)?;
    ats.close().map_err(text)?;
    group.close().map_err(text)?;
    writer.close().map_err(text)?;
    Ok((key, bytes::Bytes::from(buffer)))
}

/// The newest files, which planning samples, hold an INT96 under an embedded type the direct route allows.
pub(crate) fn int96_newest() -> Result<Vec<(String, bytes::Bytes)>, String> {
    Ok(vec![int96_file(
        "day=2026-11-01/part-0.parquet".to_owned(),
    )?])
}

/// A full batch of microseconds, a batch of INT96 under embedded microseconds, then a batch of microseconds, the newest files planning samples.
pub(crate) fn int96_in_a_later_batch() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files: Vec<(String, bytes::Bytes)> = Vec::new();
    for index in 0..1000 {
        files.push(timestamp_file(
            format!("day=2026-11-01/part-{index:05}.parquet"),
            false,
        )?);
        files.push(int96_file(format!(
            "day=2026-11-02/part-{index:05}.parquet"
        ))?);
    }
    for index in 0..25 {
        files.push(timestamp_file(
            format!("day=2026-11-03/part-{index:05}.parquet"),
            false,
        )?);
    }
    Ok(files)
}

/// `debug` on a project whose connection has `stage`, against a fake prepared as the case says: its lines, and the statements sent.
pub(crate) async fn run_debug(
    stage: &str,
    test_case: &DebugStageTestCase,
) -> Result<(Vec<String>, Vec<String>), String> {
    let fake =
        sqlload_testkit::fake_snowflake::main::start_fake_snowflake::start_fake_snowflake().await?;
    (test_case.prepare)(&fake);
    let base = fake.base_url.clone();
    let text = format!(
        "[connections.wh]\nkind = \"snowflake\"\naccount = \"acct\"\nuser = \"loader\"\nauth = {{ method = \"pat\", token = \"t\" }}\ndatabase = \"DB\"\nbase_url = \"{base}\"\nload_method = {}\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n",
        stage.replace("FAKE", &base)
    );
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let (lines, _) = sqlload::loader::main::debug_project::debug_project(&project, true).await;
    let sent = fake.state.lock().statements.clone();
    Ok((lines, sent))
}

/// Load a files table into DuckDB; take `_sqlload_source_row` away, as in a table from before it; add a file and load again. Each row's id and row number.
pub(crate) async fn run_source_row_upgrade(
    directory: &std::path::Path,
    test_case: &SourceRowUpgradeTestCase,
) -> Result<Option<String>, String> {
    let (store, connection) = local_store("", directory);
    let folder = open_folder(&Location {
        store,
        prefix: "scores/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    let text = format!(
        "[connections.files]\n{connection}\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.scores]\nkind = \"files\"\nconnection = \"files\"\npath = \"scores/\"\nwrite_strategy = \"append\"\n{}\n",
        test_case.settings
    );
    let project_destinations =
        connect_targets(&parse_project(&text, &|_| None).map_err(|error| error.to_string())?)
            .await
            .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = project_destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    for (key, ids) in [("a.parquet", &[1_i64, 2][..]), ("b.parquet", &[3][..])] {
        let file = ParquetFile {
            key,
            ids,
            score: 1.0,
            extra: None,
        };
        write_file(&folder, key, parquet_bytes(&file)?)
            .await
            .map_err(|error| error.to_string())?;
        run_once(&text, &project_destinations, &PlanOptions::default()).await?;
        execute(
            duckdb,
            [(
                "a.parquet",
                "ALTER TABLE raw.scores DROP COLUMN _sqlload_source_row",
            )]
            .iter()
            .filter(|(when, _)| *when == key)
            .map(|(_, statement)| (*statement).to_owned())
            .collect(),
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    single_text(
        duckdb,
        "SELECT string_agg(id || ':' || coalesce(CAST(_sqlload_source_row AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.scores".to_owned(),
    )
    .await
    .map_err(|error| error.to_string())
}

/// `ORDERS` already exists, its stage refused to this role (it does not own it), and so is `@~`.
pub(crate) fn not_owned(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    orders_exists(fake);
    fake.state
        .lock()
        .denied_stages
        .extend(["@~".to_owned(), "%\"ORDERS\"".to_owned()]);
}

/// The integration, and `ORDERS` already loaded.
pub(crate) fn integration_and_orders(
    fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake,
) {
    integration(fake);
    orders_exists(fake);
}

/// `ORDERS` already exists, with the columns a load of the route files gives it.
fn orders_exists(fake: &sqlload_testkit::fake_snowflake::models::FakeSnowflake) {
    let mut state = fake.state.lock();
    state.tables.entry("ORDERS".to_owned()).or_default().columns = [
        "ID",
        "SCORE",
        "DAY",
        "_SQLLOAD_SOURCE_FILE",
        "_SQLLOAD_SOURCE_VERSION",
        "_SQLLOAD_SOURCE_ROW",
        "_SQLLOAD_LOAD_ID",
        "_SQLLOAD_LOADED_AT",
    ]
    .iter()
    .zip([
        "NUMBER",
        "FLOAT",
        "DATE",
        "TEXT",
        "TEXT",
        "NUMBER",
        "TEXT",
        "TIMESTAMP_TZ",
    ])
    .map(|(column, data_type)| ((*column).to_owned(), data_type.to_owned()))
    .collect();
}

/// `count` small Parquet files of `day` in hive form, ids from 1, with `extra` when given; `padding` lengthens their names.
pub(crate) fn day_files(
    day: &str,
    (count, padding): (usize, usize),
    extra: Option<&'static str>,
) -> Result<Vec<(String, bytes::Bytes)>, String> {
    (0..count)
        .map(|index| {
            let file = ParquetFile {
                key: "",
                ids: &[1, 2],
                score: 1.0,
                extra,
            };
            Ok((
                format!("day={day}/part-{index:05}{}.parquet", "x".repeat(padding)),
                parquet_bytes(&file)?,
            ))
        })
        .collect()
}

/// One Parquet file with a `Float16` column, which makes Snowflake reject the whole file, so the direct `COPY` never loads it.
pub(crate) fn half_float_file(key: &str) -> Result<(String, bytes::Bytes), String> {
    let ids: arrow_array::ArrayRef = std::sync::Arc::new(arrow_array::Int64Array::from(vec![1]));
    let halves = arrow_cast::cast(
        &arrow_array::Float32Array::from(vec![1.5]),
        &arrow_schema::DataType::Float16,
    )
    .map_err(|error| error.to_string())?;
    let batch = RecordBatch::try_from_iter([("id", ids), ("half", halves)])
        .map_err(|error| error.to_string())?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok((key.to_owned(), bytes::Bytes::from(buffer)))
}

/// The parity fixture `files` in one `day` folder.
fn fixture_in_a_day(
    files: sqlload_testkit::parity::models::ParityFiles,
) -> Result<Vec<(String, bytes::Bytes)>, String> {
    Ok(
        sqlload_testkit::parity::main::fixture_files::fixture_files(files)?
            .into_iter()
            .map(|(key, body)| (format!("day=2026-11-01/{key}"), body))
            .collect(),
    )
}

/// Parquet of every type the direct `COPY` loads.
pub(crate) fn every_parquet_type() -> Result<Vec<(String, bytes::Bytes)>, String> {
    fixture_in_a_day(sqlload_testkit::parity::models::ParityFiles::EveryType)
}

/// Parquet of every physical storage of those types.
pub(crate) fn every_parquet_storage() -> Result<Vec<(String, bytes::Bytes)>, String> {
    fixture_in_a_day(sqlload_testkit::parity::models::ParityFiles::StoredTypes)
}

/// NDJSON of every inferred type.
pub(crate) fn every_ndjson_type() -> Result<Vec<(String, bytes::Bytes)>, String> {
    fixture_in_a_day(sqlload_testkit::parity::models::ParityFiles::NdjsonValues)
}

/// `json` files in both layouts.
pub(crate) fn json_layouts() -> Result<Vec<(String, bytes::Bytes)>, String> {
    fixture_in_a_day(sqlload_testkit::parity::models::ParityFiles::JsonLayouts)
}

/// CSV of every inferred type.
pub(crate) fn every_csv_type() -> Result<Vec<(String, bytes::Bytes)>, String> {
    fixture_in_a_day(sqlload_testkit::parity::models::ParityFiles::CsvValues)
}

/// Load the case's files into a fresh fake Snowflake; what was planned and sent, and what stayed staged.
pub(crate) async fn run_route(
    directory: &std::path::Path,
    test_case: &RouteTestCase,
) -> Result<RouteOutcome, String> {
    let fake =
        sqlload_testkit::fake_snowflake::main::start_fake_snowflake::start_fake_snowflake().await?;
    (test_case.prepare)(&fake);
    let (root, base) = (directory.display().to_string(), fake.base_url.clone());
    let store = [
        StoreKind::Filesystem { root: root.clone() },
        StoreKind::S3 {
            bucket: "fake-bucket".to_owned(),
            region: Some("us-east-1".to_owned()),
            endpoint: Some(base.clone()),
        },
    ][usize::from(test_case.files == RouteFiles::Bucket)]
    .clone();
    let source = open_folder(&Location {
        store,
        prefix: "src/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    for (key, body) in (test_case.file_set)()? {
        write_file(&source, &key, body)
            .await
            .map_err(|error| error.to_string())?;
    }
    let lake = [
        format!("kind = \"filesystem\"\nroot = \"{root}\""),
        format!(
            "kind = \"s3\"\nbucket = \"fake-bucket\"\nregion = \"us-east-1\"\nendpoint = \"{base}\""
        ),
    ][usize::from(test_case.files == RouteFiles::Bucket)]
    .clone();
    let stage = [
        format!("load_method = {}\n", test_case.stage.replace("FAKE", &base)),
        String::new(),
    ][usize::from(test_case.stage.is_empty())]
    .clone();
    let text = format!(
        "[connections.wh]\nkind = \"snowflake\"\naccount = \"acct\"\nuser = \"loader\"\nauth = {{ method = \"pat\", token = \"t\" }}\ndatabase = \"DB\"\nbase_url = \"{base}\"\n{stage}\n[connections.lake]\n{lake}\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n\n[sources.orders]\nkind = \"files\"\nconnection = \"lake\"\npath = \"src/\"\npartition_columns = {{ day = \"date\" }}\n{}\n",
        test_case.settings
    );
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let options = PlanOptions {
        reload: test_case.reload,
        ..PlanOptions::default()
    };
    let (plan, outcome) = plan_apply_load(&project, &destinations, &options).await;
    let state = fake.state.lock();
    let ids: Vec<u64> = state.driver_queries.iter().map(|(id, _)| *id).collect();
    Ok(RouteOutcome {
        error: outcome.err().unwrap_or_default(),
        plan,
        sent: state
            .statements
            .iter()
            .chain(state.driver_queries.iter().map(|(_, statement)| statement))
            .cloned()
            .chain(
                state
                    .tables
                    .values()
                    .flat_map(|table| table.rows.iter().map(ToString::to_string)),
            )
            .collect(),
        fetches: state
            .driver_queries
            .iter()
            .filter(|(_, statement)| statement.starts_with("PUT "))
            .count(),
        staged_left: state
            .objects
            .keys()
            .filter(|key| key.starts_with("fake-bucket/_sqlload/"))
            .count(),
        sequence_ids_rise: ids.windows(2).all(|pair| pair[0] < pair[1]),
    })
}

/// Which of `wanted` no text of `texts` contains.
pub(crate) fn not_found<'a>(texts: &[String], wanted: &[&'a str]) -> Vec<&'a str> {
    wanted
        .iter()
        .copied()
        .filter(|text| !texts.iter().any(|sent| sent.contains(text)))
        .collect()
}

/// Which of `unwanted` some text of `texts` contains.
pub(crate) fn found<'a>(texts: &[String], unwanted: &[&'a str]) -> Vec<&'a str> {
    unwanted
        .iter()
        .copied()
        .filter(|text| texts.iter().any(|sent| sent.contains(text)))
        .collect()
}

/// Plan, apply and load once: the rendered plan, and what apply printed or the error text.
async fn plan_apply_load(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> (String, Result<Vec<String>, String>) {
    let mut plan_text = String::new();
    let outcome = async {
        let plan = plan_project(project, destinations, options)
            .await
            .map_err(|error| error.to_string())?;
        let (lines, ok) = render_plan(&plan);
        plan_text = lines.join("\n");
        ok.then_some(()).ok_or_else(|| plan_text.clone())?;
        let applied = apply_project(&plan, destinations)
            .await
            .map_err(|error| error.to_string())?;
        load_all(project, &plan, destinations).await?;
        Ok(applied)
    }
    .await;
    (plan_text, outcome)
}

/// Every row in the Parquet files under the table folder, with its partition folder, id, `column` (`-` when the file lacks it), load id and the column's type.
async fn written_rows(table: &Location, column: &str) -> Result<Vec<WrittenRow>, String> {
    let folder = open_folder(table).map_err(|error| error.to_string())?;
    let mut rows: Vec<WrittenRow> = Vec::new();
    for file in list_files(&folder)
        .await
        .map_err(|error| error.to_string())?
    {
        let bytes = read_file(&folder, &file.relative_key)
            .await
            .map_err(|error| error.to_string())?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .and_then(|builder| builder.build())
            .map_err(|error| error.to_string())?;
        let partition = file
            .relative_key
            .rsplit_once('/')
            .map_or("-", |(partition, _)| partition)
            .to_owned();
        for batch in reader {
            let batch = [batch.map_err(|error| error.to_string())?];
            let count = batch[0].num_rows();
            let ids = column_values(&batch, "id")?;
            let values = batch[0]
                .schema()
                .column_with_name(column)
                .map(|_| column_values(&batch, column))
                .unwrap_or_else(|| Ok(vec!["-".to_owned(); count]))?;
            let load_ids = column_values(&batch, "_sqlload_load_id")?;
            let data_type = batch[0]
                .schema()
                .column_with_name(column)
                .map(|(_, field)| field.data_type().to_string())
                .unwrap_or_default();
            rows.extend(
                ids.into_iter()
                    .zip(values)
                    .zip(load_ids)
                    .map(|((id, value), load_id)| WrittenRow {
                        folder: partition.clone(),
                        id,
                        value,
                        load_id,
                        data_type: data_type.clone(),
                    }),
            );
        }
    }
    Ok(rows)
}

/// Per partition folder: the runs whose loads wrote it, then `id:value` for each row in id order; then the column's types if files differ, and leftover pending markers.
fn written_summary(rows: Vec<WrittenRow>, runs: &HashMap<String, usize>, pending: usize) -> String {
    let types: std::collections::BTreeSet<String> = rows
        .iter()
        .map(|row| row.data_type.clone())
        .filter(|data_type| !data_type.is_empty())
        .collect();
    let mut partitions: BTreeMap<String, WrittenPartition> = BTreeMap::new();
    for row in rows {
        let entry = partitions.entry(row.folder).or_default();
        entry
            .runs
            .insert(runs.get(&row.load_id).copied().unwrap_or_default());
        entry.rows.push((
            row.id.parse().unwrap_or_default(),
            format!("{}:{}", row.id, row.value),
        ));
    }
    let described: Vec<String> = partitions
        .into_iter()
        .map(|(folder, mut partition)| {
            partition.rows.sort();
            let loads: Vec<String> = partition
                .runs
                .iter()
                .map(|run| format!("run{run}"))
                .collect();
            let values: Vec<String> = partition.rows.into_iter().map(|(_, value)| value).collect();
            format!("{folder} {} {}", loads.join("+"), values.join(","))
        })
        .collect();
    let types: Vec<String> = types.into_iter().collect();
    let notes: Vec<String> = [
        (types.len() > 1).then(|| format!(" types:{}", types.join(","))),
        (pending > 0).then(|| format!(" pending:{pending}")),
    ]
    .into_iter()
    .flatten()
    .collect();
    format!("{}{}", described.join("; "), notes.concat())
}

/// The pending markers left in the table folder.
async fn pending_markers(table: &Location) -> Result<usize, String> {
    let markers = Location {
        store: table.store.clone(),
        prefix: format!("{}_sqlload/pending/", table.prefix),
    };
    let folder = open_folder(&markers).map_err(|error| error.to_string())?;
    list_files(&folder)
        .await
        .map(|files| files.len())
        .map_err(|error| error.to_string())
}

/// Run a files target scenario on fresh `public` and `sales` schemas and a fresh folder: each run's plan and error are checked, `apply` must print nothing, and the written files are read back; returns the summary and the `public.orders` table folder.
pub(crate) async fn run_files_target_scenario(
    client: &Client,
    port: u16,
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &FilesTargetScenarioTestCase,
) -> Result<(String, Location), String> {
    client
        .batch_execute(
            "DROP SCHEMA IF EXISTS public CASCADE; DROP SCHEMA IF EXISTS sales CASCADE; CREATE SCHEMA public;",
        )
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let (store, connection) = (test_case.store)(endpoint, directory);
    let prefix = format!("lake-{}", uuid::Uuid::new_v4());
    let table = Location {
        store,
        prefix: format!("{prefix}/app/orders/"),
    };
    let table_folder = open_folder(&table).map_err(|error| error.to_string())?;
    let mut settings = test_case.settings;
    let mut runs: HashMap<String, usize> = HashMap::new();
    for (index, run) in test_case.runs.iter().enumerate() {
        settings = run.settings.unwrap_or(settings);
        let text = files_target_project(port, &connection, test_case.target_settings, settings)
            .replace("PREFIX", &prefix);
        let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        client
            .batch_execute(run.source_change)
            .await
            .map_err(|error| error.to_string())?;
        for (key, contents) in run.leftovers {
            write_file(&table_folder, key, bytes::Bytes::from(*contents))
                .await
                .map_err(|error| error.to_string())?;
        }
        let options = PlanOptions {
            reload: run.reload,
            select: run.select.iter().map(|name| (*name).to_owned()).collect(),
            ..PlanOptions::default()
        };
        let (plan, outcome) = plan_apply_load(&project, &destinations, &options).await;
        let error = outcome.as_ref().err().cloned().unwrap_or_default();
        assert!(
            error.contains(run.expected_error) && error.is_empty() == run.expected_error.is_empty(),
            "{} run {}: {error}",
            test_case.description,
            index + 1
        );
        assert!(
            plan.contains(run.expected_plan),
            "{} run {}: the plan lacks {:?}:\n{plan}",
            test_case.description,
            index + 1,
            run.expected_plan
        );
        assert!(
            outcome.unwrap_or_default().is_empty(),
            "{} run {}: apply printed lines for a files target",
            test_case.description,
            index + 1
        );
        for row in written_rows(&table, test_case.check_column).await? {
            runs.entry(row.load_id).or_insert(index + 1);
        }
    }
    let rows = written_rows(&table, test_case.check_column).await?;
    let pending = pending_markers(&table).await?;
    Ok((written_summary(rows, &runs, pending), table))
}

/// The values of partition column `column` the files source reads from the table folder's files, sorted.
pub(crate) async fn partition_values(
    table: &Location,
    column: &str,
) -> Result<Vec<String>, String> {
    let folder = open_folder(table).map_err(|error| error.to_string())?;
    let files = discover_files(
        &folder,
        &[column.to_owned()],
        &sqlload::files_source::models::FilesEncoding::default(),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut values: Vec<String> = files
        .files
        .into_iter()
        .flat_map(|file| file.partitions)
        .map(|(_, value)| value)
        .collect();
    values.sort();
    Ok(values)
}

/// Load `public.orders` from Postgres into a files target on SeaweedFS, then read those files with a files source into DuckDB, twice (running `change` in between); the files source's second plan and the check of the DuckDB table, with `{stamp}` in `check` the time between the second files load and the second DuckDB load.
pub(crate) async fn run_metadata_round_trip(
    client: &Client,
    port: u16,
    endpoint: &str,
    test_case: &MetadataRoundTripTestCase,
    check: &str,
) -> Result<(String, Option<String>), String> {
    client
        .batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let (_, connection) = seaweedfs_store(endpoint, std::path::Path::new(""));
    let prefix = format!("lake-{}", uuid::Uuid::new_v4());
    let target_text = files_target_project(port, &connection, "", "write_strategy = \"replace\"")
        .replace("PREFIX", &prefix);
    let source_text = format!(
        "[connections.lake]\n{connection}\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.orders]\nkind = \"files\"\nconnection = \"lake\"\npath = \"{prefix}/app/orders/\"\nwrite_strategy = \"replace\"\n"
    );
    let target_project =
        parse_project(&target_text, &|_| None).map_err(|error| error.to_string())?;
    let source_project =
        parse_project(&source_text, &|_| None).map_err(|error| error.to_string())?;
    let targets = connect_targets(&target_project)
        .await
        .map_err(|error| error.to_string())?;
    let duckdb_targets = connect_targets(&source_project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = duckdb_targets.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    let mut plan = String::new();
    let mut stamp = String::new();
    for change in ["", test_case.change] {
        client
            .batch_execute(change)
            .await
            .map_err(|error| error.to_string())?;
        run_once(&target_text, &targets, &PlanOptions::default()).await?;
        stamp = sqlload::time_text::main::now_utc_text::now_utc_text();
        plan = plan_text(&source_text, &duckdb_targets).await?;
        run_once(&source_text, &duckdb_targets, &PlanOptions::default()).await?;
    }
    let checked = single_text(duckdb, check.replace("{stamp}", &stamp))
        .await
        .map_err(|error| error.to_string())?;
    Ok((plan, checked))
}
