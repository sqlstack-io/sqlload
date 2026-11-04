use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use sqlload::duckdb_destination::main::execute_statements::execute_statements;
use sqlload::duckdb_destination::main::query_text::query_text;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_project::load_project;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{Destination, PlanOptions};
use sqlload::project::main::parse_project::parse_project;
use sqlload::project::models::ProjectConfig;
use sqlload::store_paths::models::{Location, StoreKind};
use sqlload_testkit::seaweedfs::constants::REGION;
use tokio_postgres::Client;

use crate::test_types::{
    FilesScenarioTestCase, FilesTargetScenarioTestCase, ParquetFile, ScenarioTestCase,
    WrittenPartition, WrittenRow,
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
path = ":memory:"

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
    load_project(&project, &plan, destinations)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
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
    let Some(Destination::DuckDb(duckdb)) = destinations.get("dev") else {
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
        execute_statements(duckdb, hand_edits)
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
    query_text(duckdb, test_case.check_query.to_owned())
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

/// Run a files scenario: write each run's files under a fresh `scores-<uuid>/` folder, then plan, apply and load into a fresh DuckDB.
pub(crate) async fn run_files_scenario(
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &FilesScenarioTestCase,
) -> Result<Option<String>, String> {
    let (store, connection) = (test_case.store)(endpoint, directory);
    let prefix = format!("scores-{}/", uuid::Uuid::new_v4());
    let text = format!(
        "[connections.files]\n{}\n\n[connections.local]\nkind = \"duckdb\"\npath = \":memory:\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.scores]\nkind = \"files\"\nconnection = \"files\"\npath = \"{prefix}\"\n{}\n",
        connection, test_case.settings
    );
    let folder = open_folder(&Location { store, prefix }).map_err(|error| error.to_string())?;
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(Destination::DuckDb(duckdb)) = destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    for (index, run) in test_case.runs.iter().enumerate() {
        for file in run.files {
            write_file(&folder, file.key, parquet_bytes(file)?)
                .await
                .map_err(|error| error.to_string())?;
        }
        let error = run_once(&text, &destinations, &PlanOptions::default())
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
    query_text(duckdb, test_case.check_query.to_owned())
        .await
        .map_err(|error| error.to_string())
}

/// A project loading `public.orders` from the test Postgres into the files connection `lake`, plus `settings` under `[sources.app]`.
fn files_target_project(port: u16, connection: &str, settings: &str) -> String {
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

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.orders"]
{settings}
"#
    )
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
        load_project(project, &plan, destinations)
            .await
            .map_err(|error| error.to_string())?;
        Ok(applied)
    }
    .await;
    (plan_text, outcome)
}

/// Every row in the Parquet files under `<prefix>/app/orders/`, with its partition folder, id, `column` (`-` when the file lacks it) and load id.
async fn written_rows(
    store: &StoreKind,
    prefix: &str,
    column: &str,
) -> Result<Vec<WrittenRow>, String> {
    let folder = open_folder(&Location {
        store: store.clone(),
        prefix: format!("{prefix}/app/orders/"),
    })
    .map_err(|error| error.to_string())?;
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
            rows.extend(
                ids.into_iter()
                    .zip(values)
                    .zip(load_ids)
                    .map(|((id, value), load_id)| WrittenRow {
                        folder: partition.clone(),
                        id,
                        value,
                        load_id,
                    }),
            );
        }
    }
    Ok(rows)
}

/// Per partition folder: the runs whose loads wrote it, then `id:value` for each row in id order.
fn written_summary(rows: Vec<WrittenRow>, runs: &HashMap<String, usize>) -> String {
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
    described.join("; ")
}

/// Run a files target scenario on a fresh `public` schema and a fresh folder: each run's plan and error are checked, `apply` must print nothing, and the written files are read back.
pub(crate) async fn run_files_target_scenario(
    client: &Client,
    port: u16,
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &FilesTargetScenarioTestCase,
) -> Result<String, String> {
    client
        .batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .map_err(|error| error.to_string())?;
    client
        .batch_execute(test_case.setup)
        .await
        .map_err(|error| error.to_string())?;
    let (store, connection) = (test_case.store)(endpoint, directory);
    let prefix = format!("lake-{}", uuid::Uuid::new_v4());
    let text =
        files_target_project(port, &connection, test_case.settings).replace("PREFIX", &prefix);
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let mut runs: HashMap<String, usize> = HashMap::new();
    for (index, run) in test_case.runs.iter().enumerate() {
        client
            .batch_execute(run.source_change)
            .await
            .map_err(|error| error.to_string())?;
        let options = PlanOptions {
            reload: run.reload,
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
        for row in written_rows(&store, &prefix, test_case.check_column).await? {
            runs.entry(row.load_id).or_insert(index + 1);
        }
    }
    let rows = written_rows(&store, &prefix, test_case.check_column).await?;
    Ok(written_summary(rows, &runs))
}
