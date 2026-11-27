use std::collections::BTreeMap;
use std::sync::Arc;

use sqlload::adapters::contract::models::TableName;
use sqlload::adapters::session::errors::SessionError;
use sqlload::adapters::session::models::SessionConnection;
use sqlload::adapters::session::types::{CallerConnection, CallerSession};
use sqlload::column_types::models::Warehouse;
use sqlload::http::main::install_tls_provider::install_tls_provider;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::models::{LoadRun, PlanOptions};
use sqlload::project::main::parse_python_project::parse_python_project;
use sqlload::project::models::ProjectConfig;
use sqlload_testkit::clickhouse::constants::{
    PASSWORD as CLICKHOUSE_PASSWORD, USER as CLICKHOUSE_USER,
};
use sqlload_testkit::postgres::constants::{DATABASE, PASSWORD, USER};

use crate::test_types::{
    ClickHouseCaller, ClickHouseCallerSession, PostgresCaller, PostgresCallerSession,
};

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
pub(crate) fn message(error: &tokio_postgres::Error) -> String {
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
pub(crate) fn session_project(port: u16, settings: &str) -> Result<ProjectConfig, String> {
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

/// Plan, apply and load `project`: `ok` or the table's error, or what blocks the plan as the error.
pub(crate) async fn load(project: &ProjectConfig) -> Result<String, String> {
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
    blocked
        .is_empty()
        .then_some(())
        .ok_or_else(|| blocked.join("\n"))?;
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

impl CallerConnection for ClickHouseCaller {
    fn open(&self) -> Result<Arc<dyn CallerSession>, SessionError> {
        install_tls_provider();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SessionError::new(error.to_string()))?;
        Ok(Arc::new(ClickHouseCallerSession {
            runtime: Some(runtime),
            http: reqwest::Client::new(),
            port: self.port,
        }))
    }

    fn describe(&self) -> String {
        format!("{CLICKHOUSE_USER} on a test server")
    }
}

impl Drop for ClickHouseCallerSession {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl ClickHouseCallerSession {
    /// Send `body` to the server, a statement, or the data of `query`: its answer, or its exception's text (`Code: N. DB::Exception: ...`) as the error, as a ClickHouse client reports it.
    fn send(&self, query: Option<&str>, body: Vec<u8>) -> Result<String, SessionError> {
        on_own_thread(|| {
            let runtime = self
                .runtime
                .as_ref()
                .ok_or_else(|| SessionError::new("the session is closed"))?;
            runtime.block_on(async {
                let failed = |error: reqwest::Error| SessionError::transient(error.to_string());
                let response = self
                    .http
                    .post(format!("http://127.0.0.1:{}/", self.port))
                    .query(&query.map(|query| [("query", query)]).unwrap_or_default())
                    .header("X-ClickHouse-User", CLICKHOUSE_USER)
                    .header("X-ClickHouse-Key", CLICKHOUSE_PASSWORD)
                    .body(body)
                    .send()
                    .await
                    .map_err(failed)?;
                let status = response.status();
                let text = response.text().await.map_err(failed)?;
                match status.is_success() {
                    true => Ok(text),
                    false => Err(SessionError::new(text.trim().to_owned())),
                }
            })
        })
    }
}

impl CallerSession for ClickHouseCallerSession {
    fn execute(&self, statement: &str) -> Result<(), SessionError> {
        self.send(None, statement.as_bytes().to_vec()).map(|_| ())
    }

    fn query_text(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, SessionError> {
        let answer = self.send(None, format!("{query} FORMAT JSONCompact").into_bytes())?;
        let answer: serde_json::Value = serde_json::from_str(&answer)
            .map_err(|error| SessionError::new(format!("not JSON: {error}")))?;
        let text = |value: &serde_json::Value| match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(text) => Some(text.clone()),
            other => Some(other.to_string()),
        };
        Ok(answer["data"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|row| row.as_array().into_iter().flatten().map(text).collect())
            .collect())
    }

    fn load_parquet(&self, table: &TableName, file: &std::path::Path) -> Result<(), SessionError> {
        let data = std::fs::read(file).map_err(|error| SessionError::new(error.to_string()))?;
        let insert = format!(
            "INSERT INTO `{}`.`{}` FORMAT Parquet",
            table.schema, table.table
        );
        self.send(Some(&insert), data).map(|_| ())
    }
}

/// The project loading `public.accounts` from the Postgres on `postgres_port` into the database `raw` of the ClickHouse on `clickhouse_port`, through a caller's session on it, with `settings`.
pub(crate) fn clickhouse_session_project(
    postgres_port: u16,
    clickhouse_port: u16,
    settings: &str,
) -> Result<ProjectConfig, String> {
    let text = format!(
        r#"default_target = "raw"

[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {postgres_port}
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
            warehouse: Warehouse::ClickHouse,
            motherduck: false,
            database: None,
            caller: Arc::new(ClickHouseCaller {
                port: clickhouse_port,
            }),
        },
    )]);
    parse_python_project(&text, &|_| None, (&[], sessions)).map_err(|error| error.to_string())
}
