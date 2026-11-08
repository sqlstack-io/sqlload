use std::collections::BTreeMap;

use sqlload::adapters::clickhouse::main::stop_after::stop_after;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::PlanOptions;
use sqlload::project::models::ProjectConfig;
use sqlload_testkit::clickhouse::constants::{PASSWORD, USER};
use sqlload_testkit::clickhouse::main::run_client::run_client;
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{described_project, load_with_options, run_async};
use crate::test_types::LoadRunTestCase;

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text, balance numeric(12,2), updated_at timestamptz NOT NULL);
CREATE TABLE public.events (event_id int8, kind text, day date NOT NULL);
CREATE TABLE public.stores (code text, city text);
CREATE TABLE public.clicks (id int8, at timestamptz);
CREATE TABLE public.kinds (id int2, tz timestamptz, ntz timestamp, amount numeric(38,9), born date, ref uuid, doc jsonb, raw bytea, opens time, ok bool, ratio float8);
INSERT INTO public.accounts VALUES
  (1, 'Acme Ltd', 10.50, '2026-10-01 10:00:00+00'),
  (2, 'Beta Co', 20.00, '2026-11-02 10:00:00+00');
INSERT INTO public.events VALUES (1, 'open', '2026-10-20'), (2, 'close', '2026-10-30'), (3, 'open', '2026-11-03');
INSERT INTO public.stores VALUES ('L1', 'Leeds'), ('Y1', 'York');
INSERT INTO public.clicks VALUES (1, '2026-11-01 00:00:00+00');
INSERT INTO public.kinds VALUES
  (7, '2026-03-29 01:30:00+02', '2026-03-29 01:30:00.123456', 12345678901234567890.123456789, '1960-02-29', '0f8fad5b-d9cb-469f-a165-70867728950e', '{\"a\": 1}', '\\x00ff', '13:04:05.123456', true, 0.5),
  (8, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
";

/// The tables' settings: monthly partitions for the windowed and merged tables, a sorting key for the merged one, nothing for the rest.
const TABLES: &str = r#"
[sources.app.tables."public.accounts"]
write_strategy = "merge"
cursor_column = "updated_at"
partition_by = "toYYYYMM(updated_at)"
order_by = "id"

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "day"
partition_by = "toYYYYMM(day)"

[sources.app.tables."public.stores"]
write_strategy = "replace"
mask = { city = "hash" }

[sources.app.tables."public.clicks"]
write_strategy = "append"
cursor_column = "at"
append_cursor_inclusive = false

[sources.app.tables."public.kinds"]
write_strategy = "replace"

[sources.old]
kind = "postgres"
connection = "app_pg"
include = ["public.stores"]
schema = "legacy"
write_strategy = "replace"

[sources.old.tables."public.stores"]
description = "Stores, in a database that cannot exchange tables."
"#;

/// A project reading the test Postgres into the test ClickHouse's `schema` database (and `legacy` for the `old` source).
fn clickhouse_project(postgres_port: u16, clickhouse_port: u16, schema: &str) -> String {
    format!(
        r#"
[connections.app_pg]
kind = "postgres"
host = "127.0.0.1"
port = {postgres_port}
user = "sqlload"
password = "sqlload"
database = "app"

[connections.serving]
kind = "clickhouse"
host = "127.0.0.1"
port = {clickhouse_port}
user = "{USER}"
password = "{PASSWORD}"

[targets.dev]
connection = "serving"
loader_schema = "{schema}"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
{TABLES}
"#
    )
}

/// Plan, apply and load `selected` with `options`; the load's error, if any.
async fn load(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<(), String> {
    let plan = plan_project(project, destinations, options)
        .await
        .map_err(|error| error.to_string())?;
    let (lines, ok) = render_plan(&plan);
    ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
    apply_project(&plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    load_with_options(project, &plan, destinations, options).await
}

fn only(table: &str) -> PlanOptions {
    PlanOptions {
        select: vec![table.to_owned()],
        ..PlanOptions::default()
    }
}

#[test]
fn given_postgres_tables_when_loading_repeatedly_into_clickhouse_then_each_strategy_keeps_the_table_right()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let clickhouse = start_clickhouse().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        run_client(
            &clickhouse,
            "CREATE DATABASE legacy ENGINE = Ordinary",
            &["--allow_deprecated_database_ordinary=1"],
        )
        .await?;
        let project =
            described_project(&clickhouse_project(postgres.port, clickhouse.port, "raw")).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(serving) = destinations.get("dev") else {
            return Err("no ClickHouse destination".to_owned());
        };
        let test_cases = [
            LoadRunTestCase {
                description: "the first load creates each table with its layout: partitions, sorting key and Nullable columns",
                source_change: "",
                check_query: "SELECT partition_key || ' / ' || sorting_key || ' / ' || engine FROM system.tables WHERE database = 'raw' AND name = 'app__accounts'",
                expected_result: "toYYYYMM(updated_at) / id / MergeTree",
            },
            LoadRunTestCase {
                description: "the type matrix: each portable type in its ClickHouse column",
                source_change: "",
                check_query: "SELECT arrayStringConcat(groupArray(name || ' ' || type), ', ') FROM (SELECT name, type FROM system.columns WHERE database = 'raw' AND table = 'app__kinds' ORDER BY position)",
                expected_result: "id Nullable(Int16), tz Nullable(DateTime64(6, 'UTC')), ntz Nullable(DateTime64(6)), amount Nullable(Decimal(38, 9)), born Nullable(Date32), ref Nullable(UUID), doc Nullable(String), raw Nullable(String), opens Nullable(String), ok Nullable(Bool), ratio Nullable(Float64), _sqlload_load_id Nullable(String), _sqlload_loaded_at Nullable(DateTime64(6, 'UTC'))",
            },
            LoadRunTestCase {
                description: "the type matrix: an offset becomes its UTC instant, decimals keep every digit, dates before 1970 and NULLs stay",
                source_change: "",
                check_query: "SELECT arrayStringConcat(groupArray(row), ' | ') FROM (SELECT concat(toString(id), ';', ifNull(toString(tz), 'null'), ';', ifNull(toString(ntz), 'null'), ';', ifNull(toString(amount), 'null'), ';', ifNull(toString(born), 'null'), ';', ifNull(toString(ref), 'null'), ';', ifNull(doc, 'null'), ';', ifNull(hex(raw), 'null'), ';', ifNull(opens, 'null'), ';', ifNull(toString(ok), 'null'), ';', ifNull(toString(ratio), 'null')) AS row FROM raw.app__kinds ORDER BY id)",
                expected_result: "7;2026-03-28 23:30:00.000000;2026-03-29 01:30:00.123456;12345678901234567890.123456789;1960-02-29;0f8fad5b-d9cb-469f-a165-70867728950e;{\"a\": 1};00FF;13:04:05.123456;true;0.5 | 8;null;null;null;null;null;null;null;null;null;null",
            },
            LoadRunTestCase {
                description: "merge upserts a changed row into its own month and keeps the other month's rows",
                source_change: "UPDATE public.accounts SET balance = 99.99, updated_at = '2026-11-03 10:00:00+00' WHERE id = 2; INSERT INTO public.accounts VALUES (3, 'Gamma', 1.00, '2026-11-03 11:00:00+00');",
                check_query: "SELECT arrayStringConcat(groupArray(r), ',') FROM (SELECT toString(id) || '=' || toString(balance) AS r FROM raw.app__accounts ORDER BY id)",
                expected_result: "1=10.5,2=99.99,3=1",
            },
            LoadRunTestCase {
                description: "merge moves a row whose key changes month: the old month loses it, readers need no FINAL",
                source_change: "UPDATE public.accounts SET balance = 5.00, updated_at = '2026-12-01 00:00:00+00' WHERE id = 3;",
                check_query: "SELECT arrayStringConcat(groupArray(r), ',') FROM (SELECT toString(id) || '@' || toString(toYYYYMM(updated_at)) || '=' || toString(balance) AS r FROM raw.app__accounts ORDER BY id)",
                expected_result: "1@202610=10.5,2@202611=99.99,3@202612=5",
            },
            LoadRunTestCase {
                description: "delete_insert replaces the rows from the cursor onwards without duplicates",
                source_change: "INSERT INTO public.events VALUES (4, 'reopen', '2026-11-03');",
                check_query: "SELECT arrayStringConcat(groupArray(r), ',') FROM (SELECT toString(event_id) || ':' || kind AS r FROM raw.app__events ORDER BY event_id)",
                expected_result: "1:open,2:close,3:open,4:reopen",
            },
            LoadRunTestCase {
                description: "append adds the new rows' partition and keeps the old",
                source_change: "INSERT INTO public.clicks VALUES (2, '2026-11-02 00:00:00+00');",
                check_query: "SELECT toString(count()) || '/' || toString(uniqExact(id)) FROM raw.app__clicks",
                expected_result: "2/2",
            },
            LoadRunTestCase {
                description: "replace mirrors deletes in the source by exchanging a whole table in, and keeps hashed values",
                source_change: "DELETE FROM public.stores WHERE code = 'Y1';",
                check_query: "SELECT arrayStringConcat(groupArray(r), ',') FROM (SELECT code || '@' || city AS r FROM raw.app__stores ORDER BY code)",
                expected_result: "L1@8593b9db65212160979d946950601c26622a219c80f1f122679eb69ec5b96600",
            },
            LoadRunTestCase {
                description: "replace into a database that cannot exchange tables swaps by renames instead",
                source_change: "",
                check_query: "SELECT (SELECT engine FROM system.databases WHERE name = 'legacy') || ': ' || (SELECT arrayStringConcat(groupArray(code), ',') FROM (SELECT code FROM legacy.old__stores ORDER BY code))",
                expected_result: "Ordinary: L1",
            },
            LoadRunTestCase {
                description: "a new source column is added to the table and filled",
                source_change: "ALTER TABLE public.stores ADD COLUMN opened date; UPDATE public.stores SET opened = '2020-01-02';",
                check_query: "SELECT arrayStringConcat(groupArray(code || '@' || toString(opened)), ',') FROM raw.app__stores",
                expected_result: "L1@2020-01-02",
            },
            LoadRunTestCase {
                description: "no staging, delta or copy table is left behind",
                source_change: "",
                check_query: "SELECT toString(count()) FROM system.tables WHERE database IN ('raw', 'legacy') AND name LIKE '%sqlload_staging%'",
                expected_result: "0",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            load(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let result = single_text(serving, test_case.check_query.to_owned())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            assert_eq!(
                result.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

/// The rows of `query`'s single text, read on `serving`.
async fn read(serving: &Destination, query: &str) -> Result<String, String> {
    Ok(single_text(serving, query.to_owned())
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or_default())
}

#[test]
fn given_a_load_stopped_at_every_statement_when_it_runs_again_then_the_table_was_never_half_published_and_converges()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let clickhouse = start_clickhouse().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let project =
            described_project(&clickhouse_project(postgres.port, clickhouse.port, "stops")).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(serving) = destinations.get("dev") else {
            return Err("no ClickHouse destination".to_owned());
        };
        // A window with a lookahead, today−7 … today+7 style: it spans October and November, so its publish rebuilds two partitions at once.
        let window = PlanOptions {
            start_cursor: Some("2026-10-25".to_owned()),
            end_cursor: Some("2026-11-10".to_owned()),
            ..only("app__events")
        };
        let events = "SELECT arrayStringConcat(groupArray(r), ',') FROM (SELECT toString(event_id) || ':' || kind || '@' || toString(day) AS r FROM stops.app__events ORDER BY event_id, kind)";
        let stores = "SELECT arrayStringConcat(groupArray(code), ',') FROM (SELECT code FROM stops.app__stores ORDER BY code)";
        // Each table switches between two source states; a stopped run must leave it in one of them, and the rerun in the new one. The October row before the window is kept rows of a rebuilt partition.
        let scenarios = [
            (
                "app__events",
                8,
                window,
                events,
                [
                    (
                        "DELETE FROM public.events; INSERT INTO public.events VALUES (1, 'open', '2026-10-20'), (2, 'close', '2026-10-30'), (3, 'open', '2026-11-03');",
                        "1:open@2026-10-20,2:close@2026-10-30,3:open@2026-11-03",
                    ),
                    (
                        "DELETE FROM public.events; INSERT INTO public.events VALUES (1, 'open', '2026-10-20'), (2, 'shut', '2026-10-30'), (5, 'late', '2026-11-08');",
                        "1:open@2026-10-20,2:shut@2026-10-30,5:late@2026-11-08",
                    ),
                ],
            ),
            (
                "app__stores",
                3,
                only("app__stores"),
                stores,
                [
                    (
                        "DELETE FROM public.stores; INSERT INTO public.stores VALUES ('L1', 'Leeds'), ('Y1', 'York');",
                        "L1,Y1",
                    ),
                    (
                        "DELETE FROM public.stores; INSERT INTO public.stores VALUES ('B1', 'Bath'), ('Y1', 'York');",
                        "B1,Y1",
                    ),
                ],
            ),
        ];
        for (table, minimum, options, query, states) in scenarios {
            // The stop matches the table's qualified name, which its staging, delta and copy tables start with; this test's own database keeps it out of the other test, which runs at the same time.
            let stopped_name = format!("`stops`.`{table}");
            load(&project, &destinations, &only(table)).await?;
            assert_eq!(
                read(serving, query).await?,
                states[0].1,
                "{table}: the first load"
            );
            let mut stops = 0;
            loop {
                let (change, expected) = states[(stops + 1) % 2];
                let current = states[stops % 2].1;
                client
                    .batch_execute(change)
                    .await
                    .map_err(|error| error.to_string())?;
                stop_after(&stopped_name, Some(stops));
                let stopped = load(&project, &destinations, &options).await;
                stop_after(&stopped_name, None);
                let seen = read(serving, query).await?;
                assert!(
                    seen == current || seen == expected,
                    "{table}: a run stopped after {stops} statements left the table half published: {seen}"
                );
                if stopped.is_ok() {
                    assert_eq!(seen, expected, "{table}: the run that was never stopped");
                    break;
                }
                load(&project, &destinations, &options)
                    .await
                    .map_err(|error| format!("{table}: the rerun after {stops}: {error}"))?;
                assert_eq!(
                    read(serving, query).await?,
                    expected,
                    "{table}: the rerun after a stop at {stops} statements"
                );
                stops += 1;
            }
            assert!(
                stops >= minimum,
                "{table}: only {stops} stop points were tried"
            );
        }
        Ok(())
    })
}
