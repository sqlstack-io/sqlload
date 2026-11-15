use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::cursor_helpers::run_typed_cursors;
use crate::helpers::run_async;
use crate::test_types::{TypedCursorRun, TypedCursorTestCase};

/// A timestamp, a date and an integer cursor table, and a text cursor table.
const FAMILIES: &str = "
CREATE TABLE public.events (id int8 PRIMARY KEY, v text NOT NULL, updated_at timestamptz NOT NULL);
INSERT INTO public.events VALUES (1, 'a', '2026-11-01 10:00:00+00'), (2, 'b', '2026-11-02 10:00:00+00'), (3, 'c', '2026-11-03 10:00:00+00');
CREATE TABLE public.days (id int8 PRIMARY KEY, v text NOT NULL, day date NOT NULL);
INSERT INTO public.days VALUES (1, 'a', '2026-11-01'), (2, 'b', '2026-11-02'), (3, 'c', '2026-11-03');
CREATE TABLE public.ledger (fiscal_year int4 NOT NULL, period int4 NOT NULL, amount int8 NOT NULL, PRIMARY KEY (fiscal_year, period));
INSERT INTO public.ledger VALUES (2025, 11, 100), (2025, 12, 120), (2026, 1, 10), (2026, 2, 20);
CREATE TABLE public.codes (id int8 PRIMARY KEY, code text NOT NULL);
";

const FAMILY_SETTINGS: &str = r#"include = ["public.events", "public.days", "public.ledger"]
[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "updated_at"

[sources.app.tables."public.days"]
write_strategy = "delete_insert"
cursor_column = "day"

[sources.app.tables."public.ledger"]
write_strategy = "delete_insert"
cursor_column = "fiscal_year"
"#;

const FAMILY_ROWS: &str = "SELECT
  (SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__events) || ' ' ||
  (SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__days) || ' ' ||
  (SELECT string_agg(fiscal_year || '/' || period, ',' ORDER BY fiscal_year, period) FROM raw.app__ledger)";

const LEDGER_ROWS: &str = "SELECT string_agg(fiscal_year || '/' || period || '=' || amount, ',' ORDER BY fiscal_year, period) FROM raw.app__ledger";

/// A plan of every table with no flags.
const PLAN_ONLY: TypedCursorRun = TypedCursorRun {
    source_change: "SELECT 1",
    typed: [None; 4],
    select: &[],
    expected_plan: &[],
};

/// The year-end close: FY2025 P12 and FY2026 P1–2 load, then FY2025 P12 changes at the source, and `lookback = 1` reads FY2025 again.
const WITH_LOOKBACK: [TypedCursorRun; 2] = [
    TypedCursorRun {
        expected_plan: &["fiscal_year: first run, everything"],
        ..PLAN_ONLY
    },
    TypedCursorRun {
        source_change: "UPDATE public.ledger SET amount = 125 WHERE fiscal_year = 2025 AND period = 12;",
        expected_plan: &["fiscal_year >= 2025 (warehouse maximum 2026 minus lookback 1)"],
        ..PLAN_ONLY
    },
];

/// The same year-end close without `lookback`: the read starts at FY2026.
const WITHOUT_LOOKBACK: [TypedCursorRun; 2] = [
    WITH_LOOKBACK[0],
    TypedCursorRun {
        expected_plan: &["fiscal_year >= 2026 (warehouse maximum)"],
        ..WITH_LOOKBACK[1]
    },
];

#[test]
fn given_typed_cursor_flags_when_planning_then_each_table_takes_its_cursor_family_flag()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let test_cases = [
            TypedCursorTestCase {
                description: "-ts and -int together: the timestamp and date tables take -ts, the integer table -int",
                setup: FAMILIES,
                settings: FAMILY_SETTINGS,
                runs: &[TypedCursorRun {
                    typed: [Some("2026-11-02"), None, Some("2026"), None],
                    expected_plan: &[
                        "updated_at >= 2026-11-02 00:00:00+00 (--start-cursor-ts)",
                        "day >= 2026-11-02 (--start-cursor-ts)",
                        "fiscal_year >= 2026 (--start-cursor-int)",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: FAMILY_ROWS,
                expected_outcomes: &["ok => 2,3 2,3 2026/1,2026/2"],
            },
            TypedCursorTestCase {
                description: "an end flag of each family: the reads stop below it",
                setup: FAMILIES,
                settings: FAMILY_SETTINGS,
                runs: &[TypedCursorRun {
                    typed: [None, Some("2026-11-02T00:00:00Z"), None, Some("2026")],
                    select: &["app__events", "app__ledger"],
                    expected_plan: &[
                        "updated_at: first run, everything, below 2026-11-02 00:00:00.000000+00 (--end-cursor-ts)",
                        "fiscal_year: first run, everything, below 2026 (--end-cursor-int)",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "SELECT (SELECT string_agg(CAST(id AS VARCHAR), ',') FROM raw.app__events) || ' ' || (SELECT string_agg(fiscal_year || '/' || period, ',' ORDER BY period) FROM raw.app__ledger)",
                expected_outcomes: &["ok => 1 2025/11,2025/12"],
            },
            TypedCursorTestCase {
                description: "-int alone on a timestamp table and a date table: refused, naming the cursor, its type and the flags it needs",
                setup: FAMILIES,
                settings: FAMILY_SETTINGS,
                runs: &[TypedCursorRun {
                    typed: [None, None, Some("2026"), None],
                    select: &["app__events", "app__days"],
                    expected_plan: &[
                        "--start-cursor-int is for integer cursors, but cursor updated_at is timestamp_tz; give this table --start-cursor-ts/--end-cursor-ts, or leave it out of the selection",
                        "--start-cursor-int is for integer cursors, but cursor day is date; give this table --start-cursor-ts/--end-cursor-ts",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "-ts alone on an integer table: refused",
                setup: FAMILIES,
                settings: FAMILY_SETTINGS,
                runs: &[TypedCursorRun {
                    typed: [Some("2026-11-02"), None, None, None],
                    select: &["app__ledger"],
                    expected_plan: &[
                        "--start-cursor-ts is for date and timestamp cursors, but cursor fiscal_year is int; give this table --start-cursor-int/--end-cursor-int",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "a time of day for a date cursor: refused, with the date to give",
                setup: FAMILIES,
                settings: FAMILY_SETTINGS,
                runs: &[TypedCursorRun {
                    typed: [Some("2026-11-02T10:00:00Z"), None, None, None],
                    select: &["app__days"],
                    expected_plan: &[
                        "--start-cursor-ts 2026-11-02T10:00:00Z has a time of day, but cursor day is a date; give a date, such as 2026-11-02",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "a text cursor takes only the untyped flags",
                setup: FAMILIES,
                settings: "include = [\"public.codes\"]\n[sources.app.tables.\"public.codes\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"code\"",
                runs: &[TypedCursorRun {
                    typed: [Some("2026-11-02"), None, None, None],
                    expected_plan: &[
                        "cursor code is text, which takes --start-cursor/--end-cursor, not --start-cursor-ts",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
        ];
        for test_case in &test_cases {
            let outcomes = run_typed_cursors(&client, postgres.port, test_case).await?;
            assert_eq!(
                outcomes, test_case.expected_outcomes,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_fiscal_year_cursor_when_last_year_changes_after_the_year_end_then_lookback_one_reloads_it()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let test_cases = [
            TypedCursorTestCase {
                description: "lookback = 1 reads from the destination maximum minus one year, so the change to FY2025 lands",
                setup: FAMILIES,
                settings: r#"include = ["public.ledger"]
[sources.app.tables."public.ledger"]
write_strategy = "delete_insert"
cursor_column = "fiscal_year"
lookback = 1
"#,
                runs: &WITH_LOOKBACK,
                check_query: LEDGER_ROWS,
                expected_outcomes: &[
                    "ok => 2025/11=100,2025/12=120,2026/1=10,2026/2=20",
                    "ok => 2025/11=100,2025/12=125,2026/1=10,2026/2=20",
                ],
            },
            TypedCursorTestCase {
                description: "without lookback the read starts at FY2026, so FY2025 P12 stays stale",
                setup: FAMILIES,
                settings: r#"include = ["public.ledger"]
[sources.app.tables."public.ledger"]
write_strategy = "delete_insert"
cursor_column = "fiscal_year"
"#,
                runs: &WITHOUT_LOOKBACK,
                check_query: LEDGER_ROWS,
                expected_outcomes: &[
                    "ok => 2025/11=100,2025/12=120,2026/1=10,2026/2=20",
                    "ok => 2025/11=100,2025/12=120,2026/1=10,2026/2=20",
                ],
            },
        ];
        for test_case in &test_cases {
            let outcomes = run_typed_cursors(&client, postgres.port, test_case).await?;
            assert_eq!(
                outcomes, test_case.expected_outcomes,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_lookback_that_does_not_fit_the_cursor_when_planning_then_plan_refuses_it()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let test_cases = [
            TypedCursorTestCase {
                description: "an integer lookback on a timestamp cursor",
                setup: FAMILIES,
                settings: "include = [\"public.events\"]\n[sources.app.tables.\"public.events\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"\nlookback = 1",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback = 1 counts integer cursor units, but cursor updated_at is timestamp_tz; use a duration, such as lookback = \"1d\"",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "a duration on an integer cursor",
                setup: FAMILIES,
                settings: "include = [\"public.ledger\"]\n[sources.app.tables.\"public.ledger\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"fiscal_year\"\nlookback = \"1d\"",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback = \"1day\" is a duration, but cursor fiscal_year is an integer; use a number of cursor units, such as lookback = 1",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "any lookback on a text cursor",
                setup: FAMILIES,
                settings: "include = [\"public.codes\"]\n[sources.app.tables.\"public.codes\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"code\"\nlookback = 1",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback needs a date, timestamp or integer cursor, but cursor code is text; leave lookback out",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "zero",
                setup: FAMILIES,
                settings: "include = [\"public.ledger\"]\n[sources.app.tables.\"public.ledger\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"fiscal_year\"\nlookback = 0",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback = 0 counts back no cursor units; give 1 or more, or leave lookback out",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "a negative number",
                setup: FAMILIES,
                settings: "include = [\"public.ledger\"]\n[sources.app.tables.\"public.ledger\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"fiscal_year\"\nlookback = -1",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback = -1 counts back no cursor units; give 1 or more, or leave lookback out",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "an integer lookback with append",
                setup: FAMILIES,
                settings: "include = [\"public.ledger\"]\n[sources.app.tables.\"public.ledger\"]\nwrite_strategy = \"append\"\ncursor_column = \"fiscal_year\"\nlookback = 1",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback re-inserts the window on every run with append; use delete_insert (no key needed) or merge",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
            TypedCursorTestCase {
                description: "an integer lookback counted from now",
                setup: FAMILIES,
                settings: "include = [\"public.ledger\"]\n[sources.app.tables.\"public.ledger\"]\nwrite_strategy = \"delete_insert\"\ncursor_column = \"fiscal_year\"\nlookback = 1\nlookback_from = \"now\"",
                runs: &[TypedCursorRun {
                    expected_plan: &[
                        "lookback_from = \"now\" counts lookback back from the time of the run, so fiscal_year needs a duration lookback, such as lookback = \"1d\", not a number of cursor units",
                    ],
                    ..PLAN_ONLY
                }],
                check_query: "",
                expected_outcomes: &["refused => "],
            },
        ];
        for test_case in &test_cases {
            let outcomes = run_typed_cursors(&client, postgres.port, test_case).await?;
            assert_eq!(
                outcomes, test_case.expected_outcomes,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
