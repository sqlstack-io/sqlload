use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{run_async, run_scenario};
use crate::test_types::{ScenarioRun, ScenarioTestCase};

const OK: ScenarioRun = ScenarioRun {
    source_change: "",
    destination_change: "",
    expected_error: "",
    reload: false,
    window: (None, None),
};

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
                    ..OK
                },
                ScenarioRun {
                    source_change: "DELETE FROM public.prices WHERE id = 3;",
                    destination_change: "",
                    ..OK
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
                    ..OK
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
                ..OK
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
                    ..OK
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
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "--reload rebuilds a table whose source type changed incompatibly",
            setup: "CREATE TABLE public.codes (id int8 PRIMARY KEY, code int4);
                    INSERT INTO public.codes VALUES (1, 10);",
            settings: "[sources.app.tables.\"public.codes\"]\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "ALTER TABLE public.codes ALTER COLUMN code TYPE text; UPDATE public.codes SET code = 'A-10';",
                    expected_error: "code is int in the table but text in the source",
                    ..OK
                },
                ScenarioRun {
                    reload: true,
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(id || '=' || code || ':' || typeof(code), ',') FROM raw.app__codes",
            expected_result: "1=A-10:VARCHAR",
        },
        ScenarioTestCase {
            description: "a backfill window replaces only its own range with delete_insert",
            setup: "CREATE TABLE public.days (day date, total int4);
                    INSERT INTO public.days VALUES ('2026-11-01', 1), ('2026-11-02', 2), ('2026-11-03', 3);",
            settings: "[sources.app.tables.\"public.days\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"day\"\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "UPDATE public.days SET total = total * 10;",
                    window: (Some("2026-11-02"), Some("2026-11-03")),
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(CAST(day AS VARCHAR) || '=' || total, ',' ORDER BY day) FROM raw.app__days",
            expected_result: "2026-11-01=1,2026-11-02=20,2026-11-03=3",
        },
        ScenarioTestCase {
            description: "a mask naming no column stops the plan instead of loading the column in clear",
            setup: "CREATE TABLE public.users (id int8 PRIMARY KEY, \"Email\" text);",
            settings: "[sources.app.tables.\"public.users\"]\nmask = { email = \"hash\" }\n",
            runs: &[ScenarioRun {
                expected_error: "mask \"email\" is not a column of the source table (names are case-sensitive); did you mean \"Email\"?",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a masked cursor column is rejected",
            setup: "CREATE TABLE public.users (id int8 PRIMARY KEY, code text);",
            settings: "[sources.app.tables.\"public.users\"]\ncursor_column = \"code\"\nmask = { code = \"hash\" }\n",
            runs: &[ScenarioRun {
                expected_error: "a masked column cannot be the cursor_column",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a write that fails inside the transaction keeps the old rows and drops the staging table",
            setup: "CREATE TABLE public.people (id int8 PRIMARY KEY, name text);
                    INSERT INTO public.people VALUES (1, 'Ann'), (2, 'Bob');",
            settings: "[sources.app.tables.\"public.people\"]\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "UPDATE public.people SET name = NULL WHERE id = 2;",
                    destination_change: "ALTER TABLE raw.app__people ALTER COLUMN name SET NOT NULL",
                    expected_error: "NOT NULL constraint failed",
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(id || name, ',' ORDER BY id) || ' staging:' || (SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%sqlload_staging%') FROM raw.app__people",
            expected_result: "1Ann,2Bob staging:0",
        },
        ScenarioTestCase {
            description: "append into a table that lost its cursor column is blocked, and --reload recovers",
            setup: "CREATE TABLE public.ticks (seq int8, note text);
                    INSERT INTO public.ticks VALUES (1, 'a'), (2, 'b');",
            settings: "write_strategy = \"append\"\ncursor_column = \"seq\"\nappend_cursor_inclusive = false\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[
                OK,
                ScenarioRun {
                    destination_change: "ALTER TABLE raw.app__ticks DROP COLUMN seq",
                    expected_error: "run it once with --reload",
                    ..OK
                },
                ScenarioRun {
                    reload: true,
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(seq || note, ',' ORDER BY seq) FROM raw.app__ticks",
            expected_result: "1a,2b",
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
