use sqlload::duckdb_destination::main::execute_statements::execute_statements;
use sqlload::duckdb_destination::main::query_text::query_text;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_project::load_project;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{Destination, PlanOptions};
use sqlload::project::main::parse_project::parse_project;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use tokio_postgres::Client;

use crate::helpers::{duckdb_project, run_async};
use crate::test_types::{ScenarioRun, ScenarioTestCase};

const OK: ScenarioRun = ScenarioRun {
    source_change: "",
    destination_change: "",
    expected_error: "",
};

/// Plan, apply and load once; the error text when any step fails.
async fn run_once(
    project_text: &str,
    destinations: &std::collections::BTreeMap<String, Destination>,
) -> Result<(), String> {
    let project = parse_project(project_text, &|_| None).map_err(|error| error.to_string())?;
    let plan = plan_project(&project, destinations, &PlanOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    let (lines, ok) = render_plan(&plan);
    if !ok {
        return Err(lines.join("\n"));
    }
    apply_project(&plan, destinations)
        .await
        .map_err(|error| error.to_string())?;
    load_project(&project, &plan, destinations)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn run_scenario(
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
        if !run.destination_change.is_empty() {
            execute_statements(duckdb, vec![run.destination_change.to_owned()])
                .await
                .map_err(|error| error.to_string())?;
        }
        let error = run_once(&text, &destinations)
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

#[test]
fn given_edge_cases_when_loading_postgres_into_duckdb_then_tables_stay_correct()
-> Result<(), String> {
    let test_cases = [
        ScenarioTestCase {
            description: "a load that fails mid-read leaves the table as it was, and a rerun recovers",
            setup: "CREATE TABLE public.prices (id int8 PRIMARY KEY, amount numeric(10,2), updated_at timestamptz NOT NULL);
                    INSERT INTO public.prices VALUES (1, 1.00, '2026-11-01 00:00:00+00'), (2, 2.00, '2026-11-01 00:00:00+00');",
            settings: "[sources.app.tables.\"public.prices\"]\ncursor_column = \"updated_at\"\ncolumns = { amount = \"decimal(10,2)\" }\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "ALTER TABLE public.prices ALTER COLUMN amount TYPE numeric;
                                    UPDATE public.prices SET amount = 5.00, updated_at = '2026-11-02 00:00:00+00' WHERE id = 1;
                                    INSERT INTO public.prices VALUES (3, 'NaN', '2026-11-02 00:00:00+00');",
                    destination_change: "",
                    expected_error: "NaN and infinite numerics cannot be loaded",
                },
                ScenarioRun {
                    source_change: "DELETE FROM public.prices WHERE id = 3;",
                    destination_change: "",
                    expected_error: "",
                },
            ],
            check_query: "SELECT string_agg(id || '=' || amount, ',' ORDER BY id) || ' staging:' || (SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%sqlload_staging%') FROM raw.app__prices",
            expected_result: "1=5.00,2=2.00 staging:0",
        },
        ScenarioTestCase {
            description: "append_cursor_inclusive = false skips rows equal to the maximum",
            setup: "CREATE TABLE public.ticks (seq int8, note text);
                    INSERT INTO public.ticks VALUES (1, 'a'), (2, 'b');",
            settings: "write_strategy = \"append\"\ncursor_column = \"seq\"\nappend_cursor_inclusive = false\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "INSERT INTO public.ticks VALUES (3, 'c');",
                    destination_change: "",
                    expected_error: "",
                },
            ],
            check_query: "SELECT string_agg(seq || note, ',' ORDER BY seq) FROM raw.app__ticks",
            expected_result: "1a,2b,3c",
        },
        ScenarioTestCase {
            description: "append with the default inclusive cursor re-reads rows on the boundary",
            setup: "CREATE TABLE public.ticks (seq int8, note text);
                    INSERT INTO public.ticks VALUES (1, 'a'), (2, 'b');",
            settings: "write_strategy = \"append\"\ncursor_column = \"seq\"\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[OK, OK],
            check_query: "SELECT string_agg(seq || note, ',' ORDER BY seq) FROM raw.app__ticks",
            expected_result: "1a,2b,2b",
        },
        ScenarioTestCase {
            description: "append with lookback is rejected before anything runs",
            setup: "CREATE TABLE public.ticks (seq int8, at timestamptz);",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\nlookback = \"1h\"\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[ScenarioRun {
                source_change: "",
                destination_change: "",
                expected_error: "lookback re-inserts the window on every run with append",
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a column dropped by hand is added back, and one added by hand is left alone",
            setup: "CREATE TABLE public.stores (code text PRIMARY KEY, city text);
                    INSERT INTO public.stores VALUES ('L1', 'Leeds');",
            settings: "[sources.app.tables.\"public.stores\"]\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "",
                    destination_change: "ALTER TABLE raw.app__stores DROP COLUMN city; ALTER TABLE raw.app__stores ADD COLUMN note VARCHAR",
                    expected_error: "",
                },
            ],
            check_query: "SELECT string_agg(code || '@' || city || '/' || coalesce(note, 'null'), ',') FROM raw.app__stores",
            expected_result: "L1@Leeds/null",
        },
        ScenarioTestCase {
            description: "new_tables = skip loads only listed tables",
            setup: "CREATE TABLE public.listed (id int8 PRIMARY KEY); CREATE TABLE public.unlisted (id int8 PRIMARY KEY);",
            settings: "new_tables = \"skip\"\n[sources.app.tables.\"public.listed\"]\n",
            runs: &[OK],
            check_query: "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "app__listed",
        },
        ScenarioTestCase {
            description: "new_tables = fail stops the plan and names the table",
            setup: "CREATE TABLE public.listed (id int8 PRIMARY KEY); CREATE TABLE public.unlisted (id int8 PRIMARY KEY);",
            settings: "new_tables = \"fail\"\n[sources.app.tables.\"public.listed\"]\n",
            runs: &[ScenarioRun {
                source_change: "",
                destination_change: "",
                expected_error: "public.unlisted: new table and new_tables = \"fail\"",
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        for test_case in &test_cases {
            let result = run_scenario(&client, postgres.port, test_case).await?;
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
