use std::collections::BTreeMap;

use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{LoadRun, PlanOptions};
use sqlload::project::models::ProjectConfig;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres_version::start_postgres_version;
use tokio_postgres::Client;

use crate::helpers::{cdc_project, duckdb_text, run_async, run_sqlload, step};
use crate::test_types::{CdcScenarioTestCase, CdcStep};

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
unique_key = "{{primary_key}}"
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

[sources.app.tables."public.keys_src"]
description = "Test text cursor, ICU-collated in the source."
write_strategy = "append"
cursor_column = "k"
append_cursor_inclusive = false

[sources.app.tables."public.keys_dst"]
description = "Test text cursor, ICU-collated in the target."
write_strategy = "append"
cursor_column = "k"
append_cursor_inclusive = false
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
checks = {{ unique = ["ID"], not_null = ["EMAIL"], min_rows = 1, max_row_drop = "50%" }}

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

/// Run `test_case` in a fresh Postgres server of its version set up by `setup`, each step against the DuckDB file, and return its check query's result.
pub(crate) fn run_cdc_scenario(
    test_case: &CdcScenarioTestCase,
    setup: &str,
) -> Result<String, String> {
    let mut observed = String::new();
    run_async(async {
        let postgres = start_postgres_version(test_case.tag).await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(setup)
            .await
            .map_err(|error| error.to_string())?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        let project = cdc_project(postgres.port, &database)
            + &test_case
                .extra_config
                .replace("{port}", &postgres.port.to_string());
        std::fs::write(&config, project).map_err(|error| error.to_string())?;
        for (index, step) in test_case.steps.iter().enumerate() {
            client
                .batch_execute(step.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let (success, output) =
                run_sqlload(&config, step.args, (step.crash_at, test_case.spill_bytes))?;
            assert_eq!(
                success, step.expected_success,
                "{}: step {index} {:?}\n{output}",
                test_case.description, step.args
            );
            assert!(
                output.contains(step.expected_output),
                "{}: step {index} {:?} output lacks {:?}\n{output}",
                test_case.description,
                step.args,
                step.expected_output
            );
        }
        observed = duckdb_text(&database, test_case.check_query)?;
        Ok(())
    })?;
    Ok(observed)
}

/// A source capturing `crm."Contacts"` under dlt's names, so its columns `ContactID`, `Full Name` and `IsActive` load as `contact_id`, `full_name` and `is_active`.
pub(crate) const DLT_CRM_SOURCE: &str = r#"
[sources.crm]
kind = "postgres_cdc"
connection = "pg"
include = ["crm.*"]
naming = "dlt"
"#;

const CRM: &[&str] = &["load", "crm"];

/// The table, its snapshot, then a transaction inserting, updating and deleting a contact, and a re-run of nothing.
pub(crate) const DLT_CRM: [CdcStep; 3] = [
    step(
        r#"
CREATE SCHEMA crm;
CREATE TABLE crm."Contacts" ("ContactID" int8 PRIMARY KEY, "Full Name" text, "IsActive" boolean);
INSERT INTO crm."Contacts" VALUES (1, 'Ann', true), (2, 'Bo', true);
"#,
        CRM,
        "",
        (true, "loaded"),
    ),
    step(
        r#"
BEGIN;
INSERT INTO crm."Contacts" VALUES (3, 'Cy', false);
UPDATE crm."Contacts" SET "Full Name" = 'Ann B' WHERE "ContactID" = 1;
DELETE FROM crm."Contacts" WHERE "ContactID" = 2;
COMMIT;
"#,
        CRM,
        "",
        (true, "Completed"),
    ),
    step("", CRM, "", (true, "0 rows")),
];

/// Each contact under its loaded names, with its last change and whether it is deleted.
pub(crate) const DLT_CRM_CHECK: &str = "SELECT string_agg(contact_id || ':' || full_name || ':' || is_active || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY contact_id) FROM raw.crm__contacts";

/// Sources beside the `postgres_cdc` ones that `load --follow` must refuse to follow with: a source read whole, one with no `write_strategy`, and a `postgres_cdc` source with a table that does not plan.
pub(crate) const PLAIN_SOURCE: &str = r#"
[sources.plain]
kind = "postgres"
connection = "pg"
include = ["public.items"]
write_strategy = "replace"

[sources.plain.tables."public.items"]
description = "Items, read whole."

[sources.unset]
kind = "postgres"
connection = "pg"
include = ["public.orders"]

[sources.half]
kind = "postgres_cdc"
connection = "pg"
include = ["public.items", "public.orders"]
publication = "half_pub"

[sources.half.tables."public.orders"]
description = "Orders, captured."
columns = { tags = "text" }

[sources.half.tables."public.items"]
description = "Items, keyed by a column outside the primary key."
unique_key = ["name"]
"#;

/// Each way `load --follow` refuses to start, and a `--max-duration` without it.
pub(crate) const FOLLOW_REFUSED: [CdcStep; 6] = [
    step(
        "",
        &["load", "app", "unset", "--follow", "--max-duration", "3s"],
        "",
        (
            false,
            "since the replication slot would move past the changes of one left out; fix the errors above, or leave them out to follow the rest: --exclude unset",
        ),
    ),
    step(
        "",
        &["load", "half", "--follow", "--max-duration", "3s"],
        "",
        (
            false,
            "fix the errors above, or leave them out to follow the rest: the failing tables of half in exclude under [sources.half]",
        ),
    ),
    step(
        "",
        &["load", "app", "log", "--follow"],
        "",
        (
            false,
            "load --follow follows one postgres_cdc source per process",
        ),
    ),
    step(
        "",
        &["load", "app", "plain", "--follow"],
        "",
        (
            false,
            "plain__items is not captured, so load them without --follow, or leave them out: --exclude plain__items",
        ),
    ),
    step(
        "",
        &["plan", "app", "--follow"],
        "",
        (false, "--follow and --max-duration apply to load only"),
    ),
    step(
        "",
        &["load", "app", "--max-duration", "3s"],
        "",
        (false, "--follow"),
    ),
];
