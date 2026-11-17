use std::path::Path;

use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use tokio_postgres::Client;

use crate::helpers::{duckdb_execute, duckdb_text};
use crate::test_types::{WideLoadTestCase, WideStep};

/// The budget the loads run under: with what SQLLoad holds at plan, 30% of it is under the 512MB DuckDB's least `memory_limit` of 256MB takes, so DuckDB gets 256MB.
pub(crate) const BUDGET: &str = "1200MB";

/// A load with nothing changed before it.
pub(crate) const LOAD: WideStep = WideStep {
    change: "",
    prepare: "",
    environment: &[],
    command: "load",
};

/// The plan before a load, whose output the test case reads.
pub(crate) const PLAN: WideStep = WideStep {
    command: "plan",
    ..LOAD
};

/// A load after every row's body changed.
pub(crate) const NEW_BODY: WideStep = WideStep {
    change: "UPDATE public.{table} SET body = repeat(md5('n' || id::text), 32768);",
    ..LOAD
};

/// A load after every row's body changed and its cursors moved past the last load's.
pub(crate) const NEW_BODY_LATER: WideStep = WideStep {
    change: "UPDATE public.{table} SET body = repeat(md5('n' || id::text), 32768), at = at + interval '400 days', at2 = at2 + interval '400 days';",
    ..LOAD
};

/// A load killed once its first piece commits (test builds only).
pub(crate) const KILLED: WideStep = WideStep {
    environment: &[("SQLLOAD_TEST_CRASH_AT", "after-first-piece")],
    ..LOAD
};

/// [`NEW_BODY`], killed once its first piece commits.
pub(crate) const NEW_BODY_KILLED: WideStep = WideStep {
    environment: KILLED.environment,
    ..NEW_BODY
};

/// A load after a marker of a load `older-load` that committed three copied rows.
pub(crate) const OLDER_MARKER: WideStep = WideStep {
    prepare: "CREATE TABLE IF NOT EXISTS raw._sqlload_unfinished (table_name VARCHAR, load_id VARCHAR, load_id_column VARCHAR, started_at TIMESTAMPTZ); INSERT INTO raw._sqlload_unfinished VALUES ('{table}', 'older-load', '_sqlload_load_id', now()); INSERT INTO raw.{table} SELECT * REPLACE ('older-load' AS _sqlload_load_id) FROM raw.{table} ORDER BY id LIMIT 3",
    ..LOAD
};

/// [`NEW_BODY`] while the watchdog reads the process as past 90% of the budget once the table holds rows (test builds only).
pub(crate) const NEW_BODY_TRIPPED: WideStep = WideStep {
    environment: &[("SQLLOAD_TEST_WATCH_TRIP", "wide_tripped")],
    ..NEW_BODY
};

/// The bytes of each source row's `body`: 32 hex digits repeated 32,768 times.
const BODY_REPEATS: u32 = 32_768;

/// The Postgres table `name` of `rows` rows whose `body` is 32 bytes `repeats` times (each row's own), `key_rows` rows to each `k`; `at` is indexed, `at2` the same times unindexed, `tag` an indexed text.
fn wide_table(name: &str, (rows, key_rows, repeats): (u32, u32, u32)) -> String {
    format!(
        "DROP TABLE IF EXISTS public.{name};
CREATE TABLE public.{name} (id int8 PRIMARY KEY, k int8 NOT NULL, tag text NOT NULL, at timestamptz NOT NULL, at2 timestamptz NOT NULL, body text NOT NULL);
CREATE INDEX ON public.{name} (at);
CREATE INDEX ON public.{name} (tag);
INSERT INTO public.{name} SELECT i, i / {key_rows}, lpad(i::text, 6, '0'), timestamptz '2026-11-01' + i * interval '10 minutes', timestamptz '2026-11-01' + i * interval '10 minutes', repeat(md5(i::text), {repeats}) FROM generate_series(1, {rows}) AS i;"
    )
}

/// A project loading the Postgres tables `tables` (each with its settings) into the DuckDB file `database`'s `raw` schema.
fn wide_project(port: u16, database: &Path, tables: &[(&str, &str)]) -> String {
    let mut text = format!(
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
database = "{}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = [{}]
table_name = "{{table}}"
"#,
        database.display(),
        tables
            .iter()
            .map(|(name, _)| format!("\"public.{name}\""))
            .collect::<Vec<String>>()
            .join(", ")
    );
    for (name, settings) in tables {
        text.push_str(&format!(
            "\n[sources.app.tables.\"public.{name}\"]\ndescription = \"Test rows.\"\n{settings}\n"
        ));
    }
    text
}

/// Run `sqlload` with `command` (`load` or `plan`) on `config` under [`BUDGET`], with `environment` set: whether it succeeded, and its output.
fn load_wide(
    config: &Path,
    (command, environment): (&str, &[(&str, &str)]),
) -> Result<(bool, String), String> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
        .arg("--config")
        .arg(config)
        .args(["--no-color", command, "--memory-limit", BUDGET])
        .envs(environment.iter().copied())
        .output()
        .map_err(|error| error.to_string())?;
    Ok((
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    ))
}

/// Each of the DuckDB file's `tables` as `rows:distinct ids:rows holding the changed body`, then the recovery markers left, `|`-separated, `none` for a missing table.
fn wide_result(database: &Path, tables: &[&str]) -> String {
    let counted = tables.iter().map(|table| {
        format!(
            "SELECT count(*) || ':' || count(DISTINCT id) || ':' || count(*) FILTER (WHERE body = repeat(md5('n' || CAST(id AS VARCHAR)), {BODY_REPEATS})) FROM raw.{table}"
        )
    });
    let markers = "SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_unfinished".to_owned();
    let values: Vec<String> = counted
        .chain([markers])
        .map(|query| duckdb_text(database, &query).unwrap_or_else(|_| "none".to_owned()))
        .collect();
    values.join("|")
}

/// Postgres for the tests, and a client on it.
pub(crate) async fn wide_postgres()
-> Result<(sqlload_testkit::postgres::models::Postgres, Client), String> {
    let postgres = start_postgres().await?;
    let client = connect(&postgres).await?;
    Ok((postgres, client))
}

/// Run `test_case`'s loads into a fresh DuckDB file under `folder`, each of its source tables made anew (those beside it narrow): each load's success and [`wide_result`] after it, and the last load's output.
pub(crate) async fn run_wide_load(
    (client, port): (&Client, u16),
    folder: &Path,
    test_case: &WideLoadTestCase,
) -> Result<(Vec<(bool, String)>, String), String> {
    let name = test_case.table;
    let mut setup = wide_table(name, (test_case.rows, test_case.key_rows, BODY_REPEATS));
    let mut tables = vec![(name, test_case.settings)];
    for beside in test_case.beside {
        setup.push_str(&wide_table(beside, (2, 1, 1)));
        tables.push((beside, "write_strategy = \"replace\""));
    }
    client
        .batch_execute(&setup)
        .await
        .map_err(|error| error.to_string())?;
    let database = folder.join(format!("{name}.duckdb"));
    let config = folder.join(format!("{name}.toml"));
    std::fs::write(&config, wide_project(port, &database, &tables))
        .map_err(|error| error.to_string())?;
    let names: Vec<&str> = tables.iter().map(|(table, _)| *table).collect();
    let mut results = Vec::new();
    let mut output = String::new();
    for step in test_case.steps {
        client
            .batch_execute(&step.change.replace("{table}", name))
            .await
            .map_err(|error| error.to_string())?;
        duckdb_execute(&database, &step.prepare.replace("{table}", name))?;
        let (succeeded, text) = load_wide(&config, (step.command, step.environment))?;
        results.push((succeeded, wide_result(&database, &names)));
        output = text;
    }
    Ok((results, output))
}

/// A test case's expected loads as [`run_wide_load`] gives them.
pub(crate) fn expected_loads(expected_results: &[(bool, &str)]) -> Vec<(bool, String)> {
    expected_results
        .iter()
        .map(|(succeeded, tables)| (*succeeded, (*tables).to_owned()))
        .collect()
}

/// The phrases of `expected_output` (`{table}` is `table`) that `output` lacks.
pub(crate) fn missing_phrases(
    expected_output: &[&str],
    (table, output): (&str, &str),
) -> Vec<String> {
    expected_output
        .iter()
        .map(|phrase| phrase.replace("{table}", table))
        .filter(|phrase| !output.contains(phrase))
        .collect()
}
