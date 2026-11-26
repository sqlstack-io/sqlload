use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

use crate::adapters::contract::models::{TableName, WriteMode, WriteTarget};
use crate::adapters::contract::types::TableWriter;
use crate::adapters::session::errors::SessionError;
use crate::adapters::session::main::_connect_session::connect_session;
use crate::adapters::session::main::_open_writer::open_writer;
use crate::adapters::session::models::SessionConnection;
use crate::adapters::session::tests::test_types::{
    Calls, DuckDbCaller, DuckDbSession, RecordingCaller, RecordingSession,
};
use crate::adapters::session::types::{CallerConnection, CallerSession};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::main::apply_project::apply_project;
use crate::loader::main::connect_targets::connect_targets;
use crate::loader::main::load_tables::load_tables;
use crate::loader::main::plan_project::plan_project;
use crate::loader::models::{LoadRun, PlanOptions};
use crate::project::main::parse_python_project::parse_python_project;
use crate::project::models::ProjectConfig;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// Each table's loaded rows, or why it failed, by table.
pub(crate) type Outcomes = BTreeMap<String, Result<u64, String>>;

/// The source's `orders` table at the start: two rows.
const ORDERS: &str = "CREATE TABLE orders (id INTEGER PRIMARY KEY, name VARCHAR, updated_at TIMESTAMP);
INSERT INTO orders VALUES (1, 'first', '2026-01-01 00:00:00'), (2, 'second', '2026-01-02 00:00:00');";

/// The loaded table's rows, as `id:name` by id.
const LOADED_ROWS: &str = "SELECT id, name FROM raw.shop__orders ORDER BY id";

/// The loaded table's data columns, in order.
const LOADED_COLUMNS: &str = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'shop__orders' AND NOT starts_with(column_name, '_sqlload_') ORDER BY ordinal_position";

/// The table every load test loads.
const LOADED_TABLE: &str = "raw.shop__orders";

/// The stack a load needs in a debug build, as the command line gives it.
const LOAD_STACK_BYTES: usize = 8 * 1024 * 1024;

impl CallerConnection for DuckDbCaller {
    fn open(&self) -> Result<Arc<dyn CallerSession>, SessionError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| SessionError::new("the connection's lock is poisoned"))?
            .try_clone()
            .map_err(|error| SessionError::new(error.to_string()))?;
        Ok(Arc::new(DuckDbSession {
            connection: Mutex::new(connection),
            calls: self.calls.clone(),
            fail_on: self.fail_on.clone(),
        }))
    }

    fn describe(&self) -> String {
        "a test's DuckDB".to_owned()
    }
}

impl CallerSession for DuckDbSession {
    fn execute(&self, statement: &str) -> Result<(), SessionError> {
        record(&self.calls, statement.to_owned())?;
        let refused = self
            .fail_on
            .as_deref()
            .is_some_and(|fail_on| statement.contains(fail_on));
        refused
            .then(|| SessionError::new(format!("refused for the test: {statement}")))
            .map_or(Ok(()), Err)?;
        self.connection
            .lock()
            .map_err(|_| SessionError::new("the session's lock is poisoned"))?
            .execute_batch(statement)
            .map_err(|error| SessionError::new(error.to_string()))
    }

    fn query_text(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, SessionError> {
        record(&self.calls, query.to_owned())?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| SessionError::new("the session's lock is poisoned"))?;
        texts(&connection, query).map_err(|error| SessionError::new(error.to_string()))
    }
}

impl CallerConnection for RecordingCaller {
    fn open(&self) -> Result<Arc<dyn CallerSession>, SessionError> {
        Ok(Arc::new(RecordingSession {
            calls: self.calls.clone(),
            autocommit: self.autocommit,
            references: self.references.clone(),
        }))
    }

    fn describe(&self) -> String {
        "a recording".to_owned()
    }
}

impl CallerSession for RecordingSession {
    fn execute(&self, statement: &str) -> Result<(), SessionError> {
        record(&self.calls, statement.to_owned())
    }

    fn query_text(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, SessionError> {
        record(&self.calls, query.to_owned())?;
        let answer = self
            .autocommit
            .filter(|_| query.contains("AUTOCOMMIT"))
            .map(|value| vec![Some("AUTOCOMMIT".to_owned()), Some(value.to_owned())]);
        let asks_references = query.contains("pg_constraint con");
        let references = self.references.iter().filter(|_| asks_references).cloned();
        Ok(answer.into_iter().chain(references).collect())
    }

    fn copy_in(&self, statement: &str, data: &[u8]) -> Result<(), SessionError> {
        let signature = String::from_utf8_lossy(data.get(..6).unwrap_or_default()).into_owned();
        record(&self.calls, format!("{statement} <- {signature}"))
    }

    fn load_parquet(&self, table: &TableName, file: &Path) -> Result<(), SessionError> {
        let exists = file.exists();
        record(
            &self.calls,
            format!(
                "load_parquet {}.{} exists={exists}",
                table.schema, table.table
            ),
        )
    }
}

/// Add `call` to `calls`.
fn record(calls: &Calls, call: String) -> Result<(), SessionError> {
    calls
        .lock()
        .map_err(|_| SessionError::new("the calls' lock is poisoned"))?
        .push(call);
    Ok(())
}

/// `query`'s rows on `connection`, each value as text.
fn texts(
    connection: &duckdb::Connection,
    query: &str,
) -> Result<Vec<Vec<Option<String>>>, duckdb::Error> {
    let wrapped = format!("SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ({query}) AS q");
    let mut statement = connection.prepare(&wrapped)?;
    let rows = statement.query_map([], |row| {
        (0..row.as_ref().column_count())
            .map(|index| row.get::<_, Option<String>>(index))
            .collect::<Result<Vec<Option<String>>, duckdb::Error>>()
    })?;
    rows.collect()
}

/// Run `setup` on the DuckDB database file at `path`, creating it.
pub(crate) fn run_sql(path: &Path, setup: &str) -> Result<(), String> {
    duckdb::Connection::open(path)
        .and_then(|connection| connection.execute_batch(setup))
        .map_err(|error| error.to_string())
}

/// A project loading `source`'s `main.orders` (with `settings`) into the target on the caller's connection, in schema `raw`.
fn orders_project(source: &Path, settings: &str) -> String {
    format!(
        "default_target = \"dev\"

[connections.shop_file]
kind = \"duckdb\"
database = {source:?}

[targets.dev]
connection = \"caller\"
loader_schema = \"raw\"

[sources.shop]
kind = \"duckdb\"
connection = \"shop_file\"
include = [\"main.orders\"]

[sources.shop.tables.\"main.orders\"]
{settings}
",
        source = source.display().to_string()
    )
}

/// `project`'s TOML parsed with a caller's connection attached as `caller`, on `warehouse`.
fn session_project(
    project: &str,
    (warehouse, caller): (Warehouse, Arc<dyn CallerConnection>),
) -> Result<ProjectConfig, String> {
    let sessions = BTreeMap::from([(
        "caller".to_owned(),
        SessionConnection {
            warehouse,
            motherduck: false,
            database: None,
            caller,
        },
    )]);
    parse_python_project(project, &|_| None, (&[], sessions)).map_err(|error| error.to_string())
}

/// Plan, apply and load `project`: each table's outcome, or the plan's problems.
async fn load(project: &ProjectConfig) -> Result<Outcomes, String> {
    let destinations = connect_targets(project)
        .await
        .map_err(|error| error.to_string())?;
    let options = PlanOptions::default();
    let plan = plan_project(project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    let problems = plan.problems.join("\n");
    plan.problems.is_empty().then_some(()).ok_or(problems)?;
    apply_project(&plan, &destinations)
        .await
        .map_err(|error| error.to_string())?;
    let run = LoadRun {
        project,
        destinations: &destinations,
        host: None,
        options: &options,
    };
    Ok(load_tables(&run, &plan, None)
        .await
        .into_iter()
        .map(|outcome| (outcome.table, outcome.loaded.map(|loaded| loaded.rows)))
        .collect())
}

/// Load `project`'s text through a fake caller on the target database at `target`, which fails the first statement containing `fail_on`: each table's outcome, and every call the caller's sessions got.
fn load_through(
    project: &str,
    target: &Path,
    fail_on: Option<&str>,
) -> Result<(Outcomes, Vec<String>), String> {
    let calls: Calls = Arc::default();
    let connection = duckdb::Connection::open(target).map_err(|error| error.to_string())?;
    let caller = Arc::new(DuckDbCaller {
        connection: Mutex::new(connection),
        calls: calls.clone(),
        fail_on: fail_on.map(str::to_owned),
    });
    let parsed = session_project(project, (Warehouse::DuckDb, caller))?;
    let outcomes = on_load_stack(move || runtime()?.block_on(load(&parsed)))?;
    let calls = calls.lock().map_err(|_| "poisoned".to_owned())?.clone();
    Ok((outcomes, calls))
}

/// Whether every table loaded; fails naming the first that did not.
fn loaded(outcomes: Outcomes) -> Result<(), String> {
    outcomes
        .into_iter()
        .map(|(table, outcome)| outcome.map_err(|error| format!("{table}: {error}")))
        .collect::<Result<Vec<u64>, String>>()
        .map(|_| ())
}

/// `query`'s rows on the database at `path`, each row's values joined by `:`.
fn rows_at(path: &Path, query: &str) -> Result<Vec<String>, String> {
    let connection = duckdb::Connection::open(path).map_err(|error| error.to_string())?;
    let rows = texts(&connection, query).map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(Option::unwrap_or_default)
                .collect::<Vec<String>>()
                .join(":")
        })
        .collect())
}

/// The source and target files of a test in `folder`, the source holding [`ORDERS`].
fn orders_files(folder: &Path) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    let (source, target) = (folder.join("shop.duckdb"), folder.join("warehouse.duckdb"));
    run_sql(&source, ORDERS)?;
    run_sql(&target, "SELECT 1")?;
    Ok((source, target))
}

/// The loaded table's rows after a load of `settings`, `change` to the source and a second load, both through a caller's session.
pub(crate) fn twice_loaded(settings: &str, change: &str) -> Result<Vec<String>, String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (source, target) = orders_files(folder.path())?;
    let project = orders_project(&source, settings);
    loaded(load_through(&project, &target, None)?.0).map_err(|error| format!("load 1, {error}"))?;
    run_sql(&source, change)?;
    loaded(load_through(&project, &target, None)?.0).map_err(|error| format!("load 2, {error}"))?;
    rows_at(&target, LOADED_ROWS)
}

/// After a load of `settings` and the source gaining a `status` column: whether a second load loaded the table, and its data columns.
pub(crate) fn after_new_column(settings: &str) -> Result<(bool, Vec<String>), String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (source, target) = orders_files(folder.path())?;
    let project = orders_project(&source, settings);
    loaded(load_through(&project, &target, None)?.0).map_err(|error| format!("load 1, {error}"))?;
    run_sql(
        &source,
        "ALTER TABLE orders ADD COLUMN status VARCHAR; INSERT INTO orders VALUES (3, 'third', '2026-01-03 00:00:00', 'open');",
    )?;
    let (outcomes, _) = load_through(&project, &target, None)?;
    let second = matches!(outcomes.get(LOADED_TABLE), Some(Ok(_)));
    Ok((second, rows_at(&target, LOADED_COLUMNS)?))
}

/// After a load of `settings`, `change` to the source, and a second load whose `COMMIT` the session refuses: the statement sent after the refused `COMMIT`, and the loaded table's rows.
pub(crate) fn refused_commit(
    settings: &str,
    change: &str,
) -> Result<(String, Vec<String>), String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (source, target) = orders_files(folder.path())?;
    let project = orders_project(&source, settings);
    loaded(load_through(&project, &target, None)?.0).map_err(|error| format!("load 1, {error}"))?;
    run_sql(&source, change)?;
    let (outcomes, calls) = load_through(&project, &target, Some("COMMIT"))?;
    loaded(outcomes)
        .is_err()
        .then_some(())
        .ok_or("the refused load succeeded")?;
    let next = calls
        .iter()
        .skip_while(|call| !call.contains("COMMIT"))
        .nth(1)
        .cloned()
        .ok_or_else(|| format!("nothing followed a COMMIT: {calls:?}"))?;
    Ok((next, rows_at(&target, LOADED_ROWS)?))
}

/// Write one batch of two rows (`id`, `name`) through a writer on `warehouse`'s session, then load the batch: every call the session got, the writer's folder written `<folder>`.
pub(crate) fn written_batch(warehouse: Warehouse) -> Result<Vec<String>, String> {
    let caller = Arc::new(RecordingCaller::default());
    let calls = caller.calls.clone();
    let connection = SessionConnection {
        warehouse,
        motherduck: false,
        database: Some("analytics".to_owned()),
        caller,
    };
    let column = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let columns = [
        column("id", ColumnType::Int),
        column("name", ColumnType::Text),
    ];
    let table = TableName {
        database: Some("analytics".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    };
    let staging = TableName {
        table: "orders__sqlload_staging_1".to_owned(),
        ..table.clone()
    };
    let target = WriteTarget {
        table: &table,
        staging: Some(&staging),
        columns: &columns,
        write_columns: &columns,
        compat: crate::load_naming::models::Compat::default(),
        load_id: "load1",
        batch: 1,
        direct: false,
        builds: false,
        constants: &[],
    };
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "name",
            Arc::new(StringArray::from(vec!["first", "second"])) as ArrayRef,
        ),
    ])
    .map_err(|error| error.to_string())?;
    let folder = on_load_stack(move || {
        runtime()?.block_on(async {
            let destination = connect_session(&connection, None)
                .await
                .map_err(|error| error.to_string())?;
            let mut writer = open_writer(&destination).map_err(|error| error.to_string())?;
            let folder = writer.folder.path().display().to_string();
            writer
                .write_batch(&target, batch)
                .await
                .map_err(|error| error.to_string())?;
            writer
                .load_batch(&target)
                .await
                .map_err(|error| error.to_string())?;
            Ok::<String, String>(folder)
        })
    })?;
    let calls = calls.lock().map_err(|_| "poisoned".to_owned())?.clone();
    Ok(calls
        .into_iter()
        .map(|call| call.replace(&folder, "<folder>"))
        .collect())
}

/// A runtime whose threads have the stack a load needs in a debug build.
fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(LOAD_STACK_BYTES)
        .build()
        .map_err(|error| error.to_string())
}

/// `body` run on a thread with the stack a load needs in a debug build, as the command line runs a load.
fn on_load_stack<T: Send>(body: impl FnOnce() -> Result<T, String> + Send) -> Result<T, String> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(LOAD_STACK_BYTES)
            .spawn_scoped(scope, body)
            .map_err(|error| error.to_string())?
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// `texts` as owned strings, as expected values compare with results.
pub(crate) fn owned(texts: &[&str]) -> Vec<String> {
    texts.iter().map(|text| (*text).to_owned()).collect()
}

/// Why `project`'s TOML, with a recording caller's connection attached as `caller`, does not parse; `None` when it does.
pub(crate) fn caller_parse_problem(project: &str) -> Option<String> {
    session_project(
        project,
        (Warehouse::DuckDb, Arc::new(RecordingCaller::default())),
    )
    .err()
}

/// Why a session on `warehouse` that says its autocommit is `autocommit` is refused when it opens; `None` when it opens.
pub(crate) fn autocommit_refusal(
    warehouse: Warehouse,
    autocommit: Option<&'static str>,
) -> Result<Option<String>, String> {
    let caller = Arc::new(RecordingCaller {
        calls: Arc::default(),
        autocommit,
        references: Vec::new(),
    });
    let connection = SessionConnection {
        warehouse,
        motherduck: false,
        database: Some("analytics".to_owned()),
        caller,
    };
    on_load_stack(move || {
        Ok(runtime()?
            .block_on(connect_session(&connection, None))
            .err()
            .map(|error| error.to_string()))
    })
}

/// What the checks before a write in `mode` into an existing Postgres table through a caller's session refuse and warn of, when one foreign key references it with `reference`: its action, whether it is deferred, and whether it is the table's own.
pub(crate) fn reference_checks(
    (action, deferred, own): (&str, bool, bool),
    mode: WriteMode,
) -> Result<(Vec<String>, Vec<String>), String> {
    use crate::adapters::dispatch::main::session::write_problems::write_problems;
    use crate::adapters::dispatch::models::Destination;

    let text = |value: &str| Some(value.to_owned());
    let flag = |value: bool| Some(value.to_string());
    let caller = Arc::new(RecordingCaller {
        calls: Arc::default(),
        autocommit: None,
        references: vec![vec![
            text("shop.order_lines"),
            text("order_lines_order_fkey"),
            text(action),
            flag(deferred),
            flag(own),
        ]],
    });
    let connection = SessionConnection {
        warehouse: Warehouse::Postgres,
        motherduck: false,
        database: None,
        caller,
    };
    let table = TableName {
        database: None,
        schema: "shop".to_owned(),
        table: "orders".to_owned(),
    };
    on_load_stack(move || {
        runtime()?.block_on(async {
            let destination = Destination::Session(Box::new(
                connect_session(&connection, None)
                    .await
                    .map_err(|error| error.to_string())?,
            ));
            write_problems(&destination, &table, (&mode, true, false))
                .await
                .map_err(|error| error.to_string())
        })
    })
}

/// A `replace`.
pub(crate) fn replace_mode() -> WriteMode {
    WriteMode::Replace
}

/// A `delete_insert` on `updated_at`.
pub(crate) fn delete_insert_mode() -> WriteMode {
    WriteMode::DeleteInsert {
        cursor_column: Some("updated_at".to_owned()),
        from: None,
        to: None,
        values: Vec::new(),
        replace_key: Vec::new(),
        not_null_keys: Vec::new(),
    }
}
