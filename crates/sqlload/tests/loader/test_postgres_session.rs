use std::collections::BTreeMap;
use std::sync::Arc;

use sqlload::adapters::contract::models::TableName;
use sqlload::adapters::session::errors::SessionError;
use sqlload::adapters::session::models::SessionConnection;
use sqlload::adapters::session::types::{CallerConnection, CallerSession};
use sqlload::column_types::models::Warehouse;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::models::{LoadRun, PlanOptions};
use sqlload::project::main::parse_python_project::parse_python_project;
use sqlload::project::models::ProjectConfig;
use sqlload_testkit::postgres::constants::{DATABASE, PASSWORD, USER};
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::run_async;
use crate::test_types::{PostgresCaller, PostgresCallerSession, PostgresSessionTestCase};

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text, updated_at timestamptz NOT NULL);
INSERT INTO public.accounts VALUES (1, 'Acme', '2026-11-01 10:00:00+00'), (2, 'Beta', '2026-11-02 10:00:00+00');
CREATE SCHEMA raw;
";

impl CallerConnection for PostgresCaller {
    fn open(&self) -> Result<Arc<dyn CallerSession>, SessionError> {
        let port = self.port;
        on_own_thread(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| SessionError::new(error.to_string()))?;
            let address = format!(
                "host=127.0.0.1 port={port} user={USER} password={PASSWORD} dbname={DATABASE}"
            );
            let (client, connection) = runtime
                .block_on(tokio_postgres::connect(&address, tokio_postgres::NoTls))
                .map_err(|error| SessionError::new(message(&error)))?;
            runtime.spawn(connection);
            Ok(Arc::new(PostgresCallerSession {
                runtime: Some(runtime),
                client,
            }) as Arc<dyn CallerSession>)
        })
    }

    fn describe(&self) -> String {
        format!("{USER} on a test server")
    }
}

impl Drop for PostgresCallerSession {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl PostgresCallerSession {
    /// `work` run to its end on the session's runtime.
    fn block_on<T>(&self, work: impl std::future::Future<Output = T>) -> Result<T, SessionError> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.block_on(work))
            .ok_or_else(|| SessionError::new("the session is closed"))
    }
}

impl CallerSession for PostgresCallerSession {
    fn execute(&self, statement: &str) -> Result<(), SessionError> {
        on_own_thread(|| {
            self.block_on(self.client.batch_execute(statement))?
                .map_err(|error| SessionError::new(message(&error)))
        })
    }

    fn query_text(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, SessionError> {
        on_own_thread(|| {
            let messages = self
                .block_on(self.client.simple_query(query))?
                .map_err(|error| SessionError::new(message(&error)))?;
            Ok(messages
                .iter()
                .filter_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                        (0..row.len())
                            .map(|index| row.get(index).map(str::to_owned))
                            .collect(),
                    ),
                    _ => None,
                })
                .collect())
        })
    }

    fn copy_in(&self, statement: &str, data: &[u8]) -> Result<(), SessionError> {
        use futures_util::SinkExt;
        on_own_thread(|| {
            self.block_on(async {
                let failed = |error: tokio_postgres::Error| SessionError::new(message(&error));
                let sink = self.client.copy_in(statement).await.map_err(failed)?;
                futures_util::pin_mut!(sink);
                sink.send(bytes::Bytes::copy_from_slice(data))
                    .await
                    .map_err(failed)?;
                sink.as_mut().finish().await.map(|_| ()).map_err(failed)
            })?
        })
    }

    fn load_parquet(
        &self,
        _table: &TableName,
        _file: &std::path::Path,
    ) -> Result<(), SessionError> {
        Err(SessionError::new("Postgres copies rows in"))
    }
}

/// What the server said, where it said anything: `tokio_postgres` shows only "db error".
fn message(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map_or_else(|| error.to_string(), |db| db.message().to_owned())
}

/// `body` run on a thread of its own, outside any async runtime, as a caller's blocking driver runs.
fn on_own_thread<T: Send>(
    body: impl FnOnce() -> Result<T, SessionError> + Send,
) -> Result<T, SessionError> {
    std::thread::scope(|scope| {
        scope
            .spawn(body)
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// The project loading `public.accounts` from the server on `port` into its schema `raw` through a caller's session, with `settings`.
fn session_project(port: u16, settings: &str) -> Result<ProjectConfig, String> {
    let text = format!(
        r#"default_target = "raw"

[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {port}
user = "{USER}"
password = "{PASSWORD}"
database = "{DATABASE}"

[targets.raw]
connection = "caller"
loader_schema = "raw"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.accounts"]
table_name = "{{table}}"

[sources.app.tables."public.accounts"]
description = "Test accounts."
{settings}
"#
    );
    let sessions = BTreeMap::from([(
        "caller".to_owned(),
        SessionConnection {
            warehouse: Warehouse::Postgres,
            motherduck: false,
            database: None,
            caller: Arc::new(PostgresCaller { port }),
        },
    )]);
    parse_python_project(&text, &|_| None, (&[], sessions)).map_err(|error| error.to_string())
}

/// Plan, apply and load `project`: `ok`, the plan's problems, or the table's error.
async fn load(project: &ProjectConfig) -> Result<String, String> {
    let destinations = connect_targets(project)
        .await
        .map_err(|error| error.to_string())?;
    let options = PlanOptions::default();
    let plan = plan_project(project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    let blocked: Vec<String> = plan
        .tables
        .iter()
        .filter_map(|table| {
            table
                .schema
                .blocked
                .as_ref()
                .map(|blocked| blocked.reasons.join("; "))
        })
        .chain(plan.problems.iter().cloned())
        .collect();
    if !blocked.is_empty() {
        return Ok(blocked.join("\n"));
    }
    apply_project(&plan, &destinations)
        .await
        .map_err(|error| error.to_string())?;
    let run = LoadRun {
        project,
        destinations: &destinations,
        host: None,
        options: &options,
    };
    let outcomes = load_tables(&run, &plan, None).await;
    Ok(outcomes
        .into_iter()
        .map(|outcome| {
            outcome
                .loaded
                .map(|_| "ok".to_owned())
                .unwrap_or_else(|error| error)
        })
        .collect::<Vec<String>>()
        .join("\n"))
}

#[test]
fn given_a_callers_postgres_session_when_loading_twice_then_rows_copy_in_and_the_checks_before_a_write_run_on_it()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            PostgresSessionTestCase {
                description: "merge copies the rows in, matching the key the table's unique index holds",
                settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"updated_at\"",
                change: "UPDATE public.accounts SET name = 'Acme Two', updated_at = '2026-11-03 10:00:00+00' WHERE id = 1",
                expected_second: "ok",
                expected_rows: &["1:Acme Two", "2:Beta"],
            },
            PostgresSessionTestCase {
                description: "replace is refused while a foreign key would cascade its deletes into another table",
                settings: "write_strategy = \"replace\"",
                change: "ALTER TABLE raw.accounts ADD PRIMARY KEY (id); CREATE TABLE public.notes (account_id int8 REFERENCES raw.accounts (id) ON DELETE CASCADE)",
                expected_second: "ON DELETE CASCADE",
                expected_rows: &["1:Acme", "2:Beta"],
            },
        ];
        for test_case in test_cases {
            let postgres = start_postgres().await?;
            let client = connect(&postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| message(&error))?;
            let project = session_project(postgres.port, test_case.settings)?;
            let first = load(&project).await?;
            client
                .batch_execute(test_case.change)
                .await
                .map_err(|error| message(&error))?;
            let second = load(&project).await?;
            let rows: Vec<String> = client
                .query(
                    "SELECT id::text || ':' || name FROM raw.accounts ORDER BY id",
                    &[],
                )
                .await
                .map_err(|error| error.to_string())?
                .iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(first, "ok", "{}", test_case.description);
            assert!(
                second.contains(test_case.expected_second),
                "{}: {second}",
                test_case.description
            );
            assert_eq!(rows, test_case.expected_rows, "{}", test_case.description);
        }
        Ok(())
    })
}
