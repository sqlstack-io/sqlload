use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use sqlload::duckdb_destination::main::execute_statements::execute_statements;
use sqlload::duckdb_destination::main::query_text::query_text;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::files_source::main::discover_files::discover_files;
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
    FilesScenario, FilesScenarioTestCase, FilesTargetScenarioTestCase, ParquetFile,
    ScenarioTestCase, TextFilesScenarioTestCase, WrittenPartition, WrittenRow, WrittenRun,
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
        "[connections.files]\n{}\n\n[connections.local]\nkind = \"duckdb\"\npath = \":memory:\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.scores]\nkind = \"files\"\nconnection = \"files\"\npath = \"{prefix}\"\n{}\n",
        connection, scenario.settings
    );
    let folder = open_folder(&Location { store, prefix }).map_err(|error| error.to_string())?;
    let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(Destination::DuckDb(duckdb)) = destinations.get("dev") else {
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
    query_text(duckdb, scenario.check_query.to_owned())
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
