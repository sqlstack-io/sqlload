use std::collections::BTreeMap;

use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{LoadRun, PlanOptions};
use sqlload::project::models::ProjectConfig;
use tokio_postgres::Client;

/// The project: the test database's `public` tables loaded into its `mart` schema, through a Postgres target on the same connection.
pub(crate) fn project_text(port: u16) -> String {
    format!(
        r#"
[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {port}
user = "sqlload"
password = "sqlload"
database = "app"

[targets.mart]
connection = "app_pg"
loader_schema = "mart"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
table_name = "{{table}}"

[sources.app.tables."public.accounts"]
description = "Test accounts."
write_strategy = "merge"
cursor_column = "updated_at"

[sources.app.tables."public.events"]
description = "Test events."
write_strategy = "delete_insert"
cursor_column = "at"

[sources.app.tables."public.visits"]
description = "Test visits."
write_strategy = "append"
cursor_column = "at"
append_cursor_inclusive = false

[sources.app.tables."public.stores"]
description = "Test stores."
write_strategy = "replace"
"#
    )
}

/// Plan, apply and load once with `options`: `ok` or the first failed table's error; the plan's lines when it fails, as the error.
pub(crate) async fn load_once(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<String, String> {
    let plan = plan_project(project, destinations, options)
        .await
        .map_err(|error| error.to_string())?;
    let (lines, ok) = render_plan(&plan);
    ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
    apply_project(&plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    let run = LoadRun {
        project,
        destinations,
        host: None,
        options,
    };
    let failures: Vec<String> = load_tables(&run, &plan, None)
        .await
        .into_iter()
        .filter_map(|outcome| outcome.loaded.err())
        .collect();
    Ok(failures.first().cloned().unwrap_or_else(|| "ok".to_owned()))
}

/// The single text value `query` returns, `NULL` for NULL.
pub(crate) async fn text(client: &Client, query: &str) -> Result<String, String> {
    client
        .query_one(query, &[])
        .await
        .and_then(|row| row.try_get::<_, Option<String>>(0))
        .map(|value| value.unwrap_or_else(|| "NULL".to_owned()))
        .map_err(|error| format!("{query}: {error}"))
}

/// The test database's `public` tables loaded through `target`'s connection into its `mart` schema, each with checks.
pub(crate) fn checks_project_text(port: u16, target: &str) -> String {
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

[targets.out]
connection = "{target}"
loader_schema = "mart"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
table_name = "{{table}}"

[sources.app.tables."public.customers"]
write_strategy = "replace"
checks = {{ unique = ["id"], not_null = ["email"], min_rows = 1, max_row_drop = "50%" }}

[sources.app.tables."public.events"]
write_strategy = "append"
cursor_column = "at"
append_cursor_inclusive = false
checks = {{ unique = ["event_id"], not_null = ["kind"] }}

[sources.app.tables."public.tags"]
write_strategy = "replace"
checks = {{ min_rows = 5, warn = ["min_rows"] }}
"#
    )
}
