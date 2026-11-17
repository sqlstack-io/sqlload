use std::path::Path;

use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use tokio_postgres::Client;

use crate::helpers::{duckdb_execute, duckdb_text};
use crate::test_types::{KilledLoadTestCase, WideLoadTestCase};

/// The budget the loads run under: with what SQLLoad holds at plan, 30% of it is under the 512MB DuckDB's least `memory_limit` of 256MB takes, so DuckDB gets 256MB.
pub(crate) const BUDGET: &str = "1200MB";

/// The bytes of each source row's `body`: 32 hex digits repeated 32,768 times.
const BODY_REPEATS: u32 = 32_768;

/// A table's `rows:distinct ids:rows holding the changed body`.
fn result_query(table: &str) -> String {
    format!(
        "SELECT count(*) || ':' || count(DISTINCT id) || ':' || count(*) FILTER (WHERE body = repeat(md5('n' || CAST(id AS VARCHAR)), {BODY_REPEATS})) FROM raw.{table}"
    )
}

/// The Postgres table `name` of `rows` rows of about 1 MiB (`body`, each row's own), `key_rows` rows to each `k`; `at` is indexed, `at2` the same times unindexed, `tag` an indexed text.
pub(crate) fn wide_table(name: &str, rows: u32, key_rows: u32) -> String {
    format!(
        "DROP TABLE IF EXISTS public.{name};
CREATE TABLE public.{name} (id int8 PRIMARY KEY, k int8 NOT NULL, tag text NOT NULL, at timestamptz NOT NULL, at2 timestamptz NOT NULL, body text NOT NULL);
CREATE INDEX ON public.{name} (at);
CREATE INDEX ON public.{name} (tag);
INSERT INTO public.{name} SELECT i, i / {key_rows}, lpad(i::text, 6, '0'), timestamptz '2026-11-01' + i * interval '10 minutes', timestamptz '2026-11-01' + i * interval '10 minutes', repeat(md5(i::text), {BODY_REPEATS}) FROM generate_series(1, {rows}) AS i;"
    )
}

/// A project loading the Postgres tables `tables` (each with its settings) into the DuckDB file `database`'s `raw` schema.
pub(crate) fn wide_project(port: u16, database: &Path, tables: &[(&str, &str)]) -> String {
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
            "\n[sources.app.tables.\"public.{name}\"]\ndescription = \"Wide rows.\"\n{settings}\n"
        ));
    }
    text
}

/// Run `sqlload load` on `config` under [`BUDGET`], with `environment` set: whether it succeeded, and its output.
pub(crate) fn load_wide(
    config: &Path,
    environment: &[(&str, &str)],
) -> Result<(bool, String), String> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
        .arg("--config")
        .arg(config)
        .args(["--no-color", "load", "--memory-limit", BUDGET])
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

/// Postgres for the tests, and a client on it.
pub(crate) async fn wide_postgres()
-> Result<(sqlload_testkit::postgres::models::Postgres, Client), String> {
    let postgres = start_postgres().await?;
    let client = connect(&postgres).await?;
    Ok((postgres, client))
}

/// A two-row table loaded beside a wide one, which must load whatever the wide one does.
const SMALL_TABLE: &str = "DROP TABLE IF EXISTS public.small; CREATE TABLE public.small (id int8, v text); INSERT INTO public.small VALUES (1, 'a'), (2, 'b');";

/// Run one [`WideLoadTestCase`] as the table `name` in a fresh DuckDB file under `folder`, beside a small table that always loads.
pub(crate) async fn run_wide_load(
    (client, port): (&Client, u16),
    folder: &Path,
    (name, test_case): (&str, &WideLoadTestCase),
) -> Result<(), String> {
    let setup = format!(
        "{}{SMALL_TABLE}",
        wide_table(name, test_case.rows, test_case.key_rows)
    );
    client
        .batch_execute(&setup)
        .await
        .map_err(|error| error.to_string())?;
    let database = folder.join(format!("{name}.duckdb"));
    let config = folder.join(format!("{name}.toml"));
    let tables = [
        (name, test_case.settings),
        ("small", "write_strategy = \"replace\""),
    ];
    std::fs::write(&config, wide_project(port, &database, &tables))
        .map_err(|error| error.to_string())?;
    let first = match test_case.change {
        Some(_) => &[][..],
        None => test_case.environment,
    };
    let (mut succeeded, mut output) = load_wide(&config, first)?;
    if let Some(change) = test_case.change {
        assert!(succeeded, "{}: the first load failed:\n{output}", test_case.description);
        client
            .batch_execute(&change.replace("{table}", name))
            .await
            .map_err(|error| error.to_string())?;
        (succeeded, output) = load_wide(&config, test_case.environment)?;
    }
    assert_eq!(
        succeeded, test_case.expected_success,
        "{}:\n{output}",
        test_case.description
    );
    for phrase in test_case.expected_output {
        assert!(
            output.contains(&phrase.replace("{table}", name)),
            "{}: no {phrase:?} in:\n{output}",
            test_case.description
        );
    }
    assert_eq!(
        (
            duckdb_text(&database, &result_query(name))?,
            duckdb_text(&database, "SELECT string_agg(v, ',' ORDER BY id) FROM raw.small")?
        ),
        (test_case.expected_result.to_owned(), "a,b".to_owned()),
        "{}: the table, and the small table beside it",
        test_case.description
    );
    Ok(())
}

/// Run one [`KilledLoadTestCase`] as the table `name`: the loads once cleanly into one DuckDB file, and into another with the last load killed after its first piece commits, then run again.
pub(crate) async fn run_killed_load(
    (client, port): (&Client, u16),
    folder: &Path,
    (name, test_case): (&str, &KilledLoadTestCase),
) -> Result<(), String> {
    let mut results = Vec::new();
    for killed in [false, true] {
        client
            .batch_execute(&wide_table(name, 300, test_case.key_rows))
            .await
            .map_err(|error| error.to_string())?;
        let database = folder.join(format!("{name}_{killed}.duckdb"));
        let config = folder.join(format!("{name}_{killed}.toml"));
        std::fs::write(
            &config,
            wide_project(port, &database, &[(name, test_case.settings)]),
        )
        .map_err(|error| error.to_string())?;
        if let Some(change) = test_case.change {
            let (succeeded, output) = load_wide(&config, &[])?;
            assert!(succeeded, "{}: the first load failed:\n{output}", test_case.description);
            client
                .batch_execute(&change.replace("{table}", name))
                .await
                .map_err(|error| error.to_string())?;
        }
        if killed {
            let crash = [("SQLLOAD_TEST_CRASH_AT", "after-first-piece")];
            let (succeeded, output) = load_wide(&config, &crash)?;
            assert!(!succeeded, "{}: the load was not killed:\n{output}", test_case.description);
            let marked = duckdb_text(
                &database,
                &format!("SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_unfinished WHERE table_name = '{name}'"),
            );
            let marked = marked.unwrap_or_else(|_| "0".to_owned());
            assert_eq!(
                marked == "1",
                test_case.expected_marker,
                "{}: the killed load's recovery marker",
                test_case.description
            );
        }
        if killed && test_case.older_marker {
            duckdb_execute(
                &database,
                &format!("CREATE TABLE IF NOT EXISTS raw._sqlload_unfinished (table_name VARCHAR, load_id VARCHAR, load_id_column VARCHAR, started_at TIMESTAMPTZ); INSERT INTO raw._sqlload_unfinished VALUES ('{name}', 'older-load', '_sqlload_load_id', now()); INSERT INTO raw.{name} SELECT * REPLACE ('older-load' AS _sqlload_load_id) FROM raw.{name} ORDER BY id LIMIT 3"),
            )?;
        }
        let (succeeded, output) = load_wide(&config, &[])?;
        assert!(succeeded, "{}: the load after it failed:\n{output}", test_case.description);
        let left = duckdb_text(
            &database,
            "SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_unfinished",
        )
        .unwrap_or_else(|_| "0".to_owned());
        assert_eq!(left, "0", "{}: markers left", test_case.description);
        results.push(duckdb_text(&database, &result_query(name))?);
    }
    assert_eq!(
        results,
        [test_case.expected_result, test_case.expected_result],
        "{}: the clean load, then the killed load and its rerun",
        test_case.description
    );
    Ok(())
}
