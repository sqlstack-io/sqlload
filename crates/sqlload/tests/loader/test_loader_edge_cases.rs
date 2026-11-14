use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{run_async, run_scenario};
use crate::test_types::{ScenarioRun, ScenarioTestCase};

const OK: ScenarioRun = ScenarioRun {
    source_change: "",
    destination_change: "",
    expected_error: "",
    expected_plan: "",
    reload: false,
    window: (None, None),
    settings: "",
    select: &[],
};

#[test]
fn given_edge_cases_when_loading_postgres_into_duckdb_then_tables_stay_correct()
-> Result<(), String> {
    let test_cases = [
        ScenarioTestCase {
            description: "a load that fails mid-read leaves the table as it was, and a rerun recovers",
            setup: "CREATE TABLE public.prices (id int8 PRIMARY KEY, amount numeric(10,2), updated_at timestamptz NOT NULL);
                    INSERT INTO public.prices VALUES (1, 1.00, '2026-11-01 00:00:00+00'), (2, 2.00, '2026-11-01 00:00:00+00');",
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.prices\"]\nwrite_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"updated_at\"\ncolumns = { amount = \"decimal(10,2)\" }\n",
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
                    expected_plan: "updated_at from 2026-11-01 00:00:00.000000+00 (destination maximum)",
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(id || '=' || amount, ',' ORDER BY id) || ' staging:' || (SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%sqlload_staging%') FROM raw.app__prices",
            expected_result: "1=5.00,2=2.00 staging:0",
        },
        ScenarioTestCase {
            description: "append reads after an integer cursor's maximum: the first run includes cursor_start, a re-run with no change loads nothing, a new row loads once",
            setup: "CREATE TABLE public.ticks (seq int8, note text);
                    INSERT INTO public.ticks VALUES (1, 'a'), (2, 'b');",
            settings: "write_strategy = \"append\"\ncursor_column = \"seq\"\ncursor_start = \"1\"\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[
                OK,
                ScenarioRun {
                    expected_plan: "seq after 2 (destination maximum)",
                    ..OK
                },
                ScenarioRun {
                    source_change: "INSERT INTO public.ticks VALUES (3, 'c');",
                    ..OK
                },
                OK,
            ],
            check_query: "SELECT string_agg(seq || note, ',' ORDER BY seq) FROM raw.app__ticks",
            expected_result: "1a,2b,3c",
        },
        ScenarioTestCase {
            description: "append reads after a timestamp cursor's maximum: a re-run with no change loads nothing, a new row loads once",
            setup: "CREATE TABLE public.ticks (at timestamptz, note text);
                    INSERT INTO public.ticks VALUES ('2026-11-01 10:00:00+00', 'a'), ('2026-11-01 11:00:00+00', 'b');",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[
                OK,
                OK,
                ScenarioRun {
                    source_change: "INSERT INTO public.ticks VALUES ('2026-11-01 11:00:01+00', 'c');",
                    ..OK
                },
                OK,
            ],
            check_query: "SELECT string_agg(note, ',' ORDER BY note) FROM raw.app__ticks",
            expected_result: "a,b,c",
        },
        ScenarioTestCase {
            description: "append_cursor_inclusive = true opts back into re-reading the maximum, and plan warns that each run appends it again",
            setup: "CREATE TABLE public.ticks (seq int8, note text);
                    INSERT INTO public.ticks VALUES (1, 'a'), (2, 'b');",
            settings: "write_strategy = \"append\"\ncursor_column = \"seq\"\nappend_cursor_inclusive = true\n[sources.app.tables.\"public.ticks\"]\n",
            runs: &[
                ScenarioRun {
                    expected_plan: "append_cursor_inclusive = true reads from the maximum seq itself, so every run appends the rows at that value again",
                    ..OK
                },
                ScenarioRun {
                    expected_plan: "seq from 2 (destination maximum)",
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(seq || note, ',' ORDER BY seq) FROM raw.app__ticks",
            expected_result: "1a,2b,2b",
        },
        ScenarioTestCase {
            description: "append with a date cursor is refused, pointing to delete_insert",
            setup: "CREATE TABLE public.days (day date, total int4);
                    INSERT INTO public.days VALUES ('2026-11-01', 1);",
            settings: "write_strategy = \"append\"\ncursor_column = \"day\"\n[sources.app.tables.\"public.days\"]\n",
            runs: &[ScenarioRun {
                expected_error: "write_strategy = \"append\" cannot follow the date cursor day: rows added later to its last day would be lost (or, with append_cursor_inclusive = true, the whole day appended again on every run); use delete_insert",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
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
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.stores\"]\n",
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
            description: "new_tables = skip: tables present at the first load are approved, and a later one loads only once include names it exactly",
            setup: "CREATE TABLE public.present (id int8 PRIMARY KEY);",
            settings: "include = [\"public.*\", \"public.approved\"]\nwrite_strategy = \"replace\"\nnew_tables = \"skip\"\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "CREATE TABLE public.later (id int8 PRIMARY KEY); CREATE TABLE public.approved (id int8 PRIMARY KEY);",
                    settings: "[sources.app.tables.\"public.later\"]\n[sources.app.tables.\"public.approved\"]\n",
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "app__approved,app__present",
        },
        ScenarioTestCase {
            description: "new_tables = skip: selecting only a later table does not make it look present at the first load",
            setup: "CREATE TABLE public.present (id int8 PRIMARY KEY);",
            settings: "write_strategy = \"replace\"\nnew_tables = \"skip\"\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "CREATE TABLE public.later (id int8 PRIMARY KEY);",
                    select: &["public.later"],
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "app__present",
        },
        ScenarioTestCase {
            description: "new_tables = fail stops the plan and names a table that appears after the first load",
            setup: "CREATE TABLE public.present (id int8 PRIMARY KEY);",
            settings: "write_strategy = \"replace\"\nnew_tables = \"fail\"\n",
            runs: &[
                OK,
                ScenarioRun {
                    source_change: "CREATE TABLE public.later (id int8 PRIMARY KEY);",
                    destination_change: "",
                    expected_error: "public.later\n    └── reason  new table and new_tables = \"fail\"; add it to include or exclude it",
                    ..OK
                },
            ],
            check_query: "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "app__present",
        },
        ScenarioTestCase {
            description: "a discovered table needs no description; one with a source comment carries it to the destination table",
            setup: "CREATE TABLE public.plain (id int8 PRIMARY KEY); CREATE TABLE public.noted (id int8 PRIMARY KEY); COMMENT ON TABLE public.noted IS 'Noted it''s here.';",
            settings: "write_strategy = \"replace\"\n",
            runs: &[OK],
            check_query: "SELECT string_agg(table_name || '=' || coalesce(comment, 'null'), ',' ORDER BY table_name) FROM duckdb_tables() WHERE schema_name = 'raw'",
            expected_result: "app__noted=Noted it's here.,app__plain=null",
        },
        ScenarioTestCase {
            description: "a table entry without a description fails the plan with where to add one",
            setup: "CREATE TABLE public.listed (id int8 PRIMARY KEY); COMMENT ON TABLE public.listed IS 'Only the source says so.';",
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.listed\"]\ndescription = \"\"\n",
            runs: &[ScenarioRun {
                expected_error: "app__listed\n    └── reason  no description; add description = \"...\" under [sources.app.tables.\"public.listed\"]",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a table named in include without a description fails the plan",
            setup: "CREATE TABLE public.named (id int8 PRIMARY KEY);",
            settings: "include = [\"public.named\"]\nwrite_strategy = \"replace\"\n",
            runs: &[ScenarioRun {
                expected_error: "app__named\n    └── reason  no description; add description = \"...\" under [sources.app.tables.\"public.named\"]",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "--reload rebuilds a table whose source type changed incompatibly",
            setup: "CREATE TABLE public.codes (id int8 PRIMARY KEY, code int4);
                    INSERT INTO public.codes VALUES (1, 10);",
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.codes\"]\n",
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
            description: "one cursor range over a timestamp and an integer cursor is refused for both, naming each table's cursor type",
            setup: "CREATE TABLE public.events (at timestamptz, n int4);
                    CREATE TABLE public.counts (at timestamptz, n int4);
                    INSERT INTO public.events VALUES ('2026-11-01', 1), ('2026-11-03', 3);
                    INSERT INTO public.counts VALUES ('2026-11-01', 1), ('2026-11-03', 3);",
            settings: "[sources.app.tables.\"public.events\"]\nwrite_strategy = \"append\"\ncursor_column = \"at\"\n[sources.app.tables.\"public.counts\"]\nwrite_strategy = \"append\"\ncursor_column = \"n\"\n",
            runs: &[ScenarioRun {
                window: (Some("2026-11-02"), None),
                expected_error: "app__counts (integer), app__events (date or timestamp)",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "varchar(n) and text cursors compare alike, so one range applies to both",
            setup: "CREATE TABLE public.codes (code varchar(8));
                    CREATE TABLE public.names (code text);
                    INSERT INTO public.codes VALUES ('a'), ('c');
                    INSERT INTO public.names VALUES ('a'), ('c');",
            settings: "[sources.app.tables.\"public.codes\"]\nwrite_strategy = \"append\"\ncursor_column = \"code\"\n[sources.app.tables.\"public.names\"]\nwrite_strategy = \"append\"\ncursor_column = \"code\"\n",
            runs: &[ScenarioRun {
                window: (Some("b"), None),
                ..OK
            }],
            check_query: "SELECT (SELECT string_agg(code, ',') FROM raw.app__codes) || '|' || (SELECT string_agg(code, ',') FROM raw.app__names)",
            expected_result: "c|c",
        },
        ScenarioTestCase {
            description: "a mask naming no column stops the plan instead of loading the column in clear",
            setup: "CREATE TABLE public.users (id int8 PRIMARY KEY, \"Email\" text);",
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.users\"]\nmask = { email = \"hash\" }\n",
            runs: &[ScenarioRun {
                expected_error: "mask \"email\" is not a column of the source table (names are case-sensitive); did you mean \"Email\"?",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "two tables dlt's naming loads into one table stop the plan, naming both",
            setup: "CREATE TABLE public.\"order-items\" (id int8); CREATE TABLE public.order_items (id int8);",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\n",
            runs: &[ScenarioRun {
                expected_error: "public.order-items and public.order_items: both load as table app__order_items under naming = \"dlt\"",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a dlt-named table that loads into the table another one loads into under SQLLoad's naming stops the plan",
            setup: "CREATE TABLE public.\"order-items\" (id int8); CREATE TABLE public.order_items (id int8);",
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.order-items\"]\nnaming = \"dlt\"\n",
            runs: &[ScenarioRun {
                expected_error: "public.order-items and public.order_items: both load as table app__order_items under naming = \"dlt\"",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a masked cursor column is rejected",
            setup: "CREATE TABLE public.users (id int8 PRIMARY KEY, code text);",
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.users\"]\nwrite_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"code\"\nmask = { code = \"hash\" }\n",
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
            settings: "write_strategy = \"replace\"\n[sources.app.tables.\"public.people\"]\n",
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
        ScenarioTestCase {
            description: "a table that neither it nor its source gives a write_strategy is refused, though it has a cursor and a primary key, naming the lines to add",
            setup: "CREATE TABLE public.keyed (id int8 PRIMARY KEY, at timestamptz NOT NULL);",
            settings: "[sources.app.tables.\"public.keyed\"]\ncursor_column = \"at\"\n",
            runs: &[ScenarioRun {
                expected_error: "public.keyed (kind = \"postgres\"): no write_strategy is set on the table or its source, and SQLLoad doesn't choose one",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "merge without a unique_key is refused, though the table has a primary key: SQLLoad no longer takes it unasked",
            setup: "CREATE TABLE public.keyed (id int8 PRIMARY KEY, note text);",
            settings: "write_strategy = \"merge\"\n",
            runs: &[ScenarioRun {
                expected_error: "write_strategy = \"merge\" needs a unique_key: the key columns, such as unique_key = [\"id\"], or unique_key = \"{primary_key}\"",
                ..OK
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
        },
        ScenarioTestCase {
            description: "a source's merge on {primary_key} covers a keyed table include matches, and a table entry that replaces overrides it, though that table has no primary key",
            setup: "CREATE TABLE public.plain (id int8, note text);
                    CREATE TABLE public.keyed (id int8 PRIMARY KEY, note text, at timestamptz NOT NULL);
                    INSERT INTO public.plain VALUES (1, 'a');
                    INSERT INTO public.keyed VALUES (1, 'a', '2026-11-01 00:00:00+00'), (2, 'a', '2026-11-01 00:00:00+00');",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\n[sources.app.tables.\"public.keyed\"]\ncursor_column = \"at\"\n[sources.app.tables.\"public.plain\"]\nwrite_strategy = \"replace\"\n",
            runs: &[
                ScenarioRun {
                    expected_plan: "merge (unique_key: id ← {primary_key})",
                    ..OK
                },
                ScenarioRun {
                    source_change: "UPDATE public.plain SET note = 'b';
                                    UPDATE public.keyed SET note = 'b', at = '2026-11-02 00:00:00+00' WHERE id = 1;",
                    expected_plan: "replace",
                    ..OK
                },
            ],
            check_query: "SELECT (SELECT string_agg(id || note, ',' ORDER BY id) FROM raw.app__plain) || '|' || (SELECT string_agg(id || note, ',' ORDER BY id) FROM raw.app__keyed)",
            expected_result: "1b|1b,2a",
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
