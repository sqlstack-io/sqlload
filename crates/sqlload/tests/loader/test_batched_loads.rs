use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::batched_helpers::{run_batched, run_gated};
use crate::helpers::run_async;
use crate::test_types::{BatchedRun, BatchedScenarioTestCase, GatedWindowsTestCase};

const IDS: &str = "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__events";

/// `public.events` reads `10 / d`, so a row with `d = 0` fails the read of the window holding it.
const WINDOWED: &str = "
CREATE TABLE public.events_raw (id int8, at date NOT NULL, d int4);
CREATE VIEW public.events AS SELECT id, at, 10 / d AS v FROM public.events_raw;
INSERT INTO public.events_raw VALUES (1, '2026-11-01', 1), (2, '2026-11-02', 1), (3, '2026-11-03', 0), (4, '2026-11-04', 1);
";

const WINDOWED_SETTINGS: &str = r#"
exclude = ["public.*_raw"]

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"
cursor_start = "2026-11-01"
batch_size = "1d"
"#;

const GATED_4_AT_ONCE: &str = r#"
exclude = ["public.*_raw"]

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"
cursor_start = "2026-11-01"
batch_size = "1d"
batch_concurrency = 4
"#;

const GATED_2_AT_ONCE: &str = r#"
exclude = ["public.*_raw"]

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"
cursor_start = "2026-11-01"
batch_size = "1d"
batch_concurrency = 2
"#;

const MICROBATCH_CONCURRENCY: &str = "microbatch_concurrency = true\n";

const WINDOWED_CONCURRENT: &str = r#"
exclude = ["public.*_raw"]

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"
cursor_start = "2026-11-01"
batch_size = "1d"
batch_concurrency = 4
"#;

/// An append table with an index on its cursor, read in order; equal cursor values must commit together.
const ORDERED: &str = "
CREATE TABLE public.events (id int8, seq int8 NOT NULL, v text);
CREATE INDEX events_seq ON public.events (seq);
INSERT INTO public.events VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 2, 'c'), (4, 3, 'd'), (5, 4, 'a'), (6, 5, 'f');
";

const ORDERED_SETTINGS: &str = r#"
exclude = ["public.*_raw"]

[sources.app.tables."public.events"]
write_strategy = "append"
cursor_column = "seq"
append_cursor_inclusive = false
"#;

/// The same table without an index: the whole read is one commit.
const UNORDERED: &str = "
CREATE TABLE public.events_raw (id int8, seq int8 NOT NULL, d int4);
CREATE VIEW public.events AS SELECT id, seq, 10 / d AS v FROM public.events_raw;
INSERT INTO public.events_raw VALUES (1, 1, 1), (2, 2, 1), (3, 3, 0), (4, 4, 1);
";

/// An integer cursor whose `batch` windows end at the source's maximum, read at plan.
const INTEGER: &str = "
CREATE TABLE public.events (id int8 NOT NULL, v text);
INSERT INTO public.events VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
";

const INTEGER_SETTINGS: &str = r#"
[sources.app.tables."public.events"]
write_strategy = "append"
cursor_column = "id"
cursor_start = "1"
append_cursor_inclusive = false
batch_size = 2
"#;

/// Timestamps whose read starts from a `cursor_start` in another form than the cursor's own text.
const TIMESTAMPS: &str = "
CREATE TABLE public.events (id int8, at timestamptz NOT NULL);
INSERT INTO public.events VALUES (1, '2026-01-01 05:00:00+00'), (2, '2026-01-02 05:00:00+00'), (3, '2026-11-01 05:00:00+00'), (4, '2026-11-02 05:00:00+00');
";

const START_AS_DATE: &str = r#"
[sources.app.tables."public.events"]
write_strategy = "append"
cursor_column = "at"
cursor_start = "2026-01-01"
batch_size = "1d"
"#;

const START_AS_RFC3339: &str = r#"
[sources.app.tables."public.events"]
write_strategy = "append"
cursor_column = "at"
cursor_start = "2026-11-01T00:00:00Z"
batch_size = "1d"
"#;

const WHOLE_SETTINGS: &str = r#"
exclude = ["public.*_raw"]

[sources.app.tables."public.events"]
write_strategy = "append"
cursor_column = "seq"
append_cursor_inclusive = false
batch_size = "none"
"#;

/// A `delete_insert` table with an index on its cursor, committing along the way.
const DAYS: &str = "
CREATE TABLE public.events (id int8, at date NOT NULL, v text);
CREATE INDEX events_at ON public.events (at);
INSERT INTO public.events VALUES (1, '2026-11-01', 'a'), (2, '2026-11-02', 'b'), (3, '2026-11-03', 'c'), (4, '2026-11-04', 'd'), (5, '2026-11-05', 'e');
";

const DAYS_SETTINGS: &str = r#"
[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"
lookback = "2d"
"#;

/// NULL cursors, which an ordered read commits first.
const NULL_CURSORS: &str = "
CREATE TABLE public.events (id int8, seq int8);
CREATE INDEX events_seq ON public.events (seq);
INSERT INTO public.events VALUES (1, 1), (2, 2), (3, NULL);
";

/// A text cursor with an index, which is still read and committed once.
const TEXT_CURSOR: &str = "
CREATE TABLE public.events (id int8, seq text NOT NULL);
CREATE INDEX events_seq ON public.events (seq);
INSERT INTO public.events VALUES (1, 'a'), (2, 'b');
";

const IGNORED_ON_REPLACE: &str = r#"
[sources.app.tables."public.events"]
write_strategy = "replace"
batch_size = "1d"
"#;

const IGNORED_WITHOUT_CURSOR: &str = r#"
[sources.app.tables."public.events"]
write_strategy = "merge"
unique_key = ["id"]
batch_size = "1d"
"#;

const NO_CURSOR: &str = "
CREATE TABLE public.events (id int8 PRIMARY KEY);
INSERT INTO public.events VALUES (1);
";

const PARALLEL: &str = "
CREATE TABLE public.a (id int8);
CREATE TABLE public.b (id int8);
CREATE TABLE public.c_raw (id int8, d int4);
CREATE VIEW public.c AS SELECT id, 10 / d AS v FROM public.c_raw;
INSERT INTO public.a VALUES (1), (2);
INSERT INTO public.b VALUES (3);
INSERT INTO public.c_raw VALUES (1, 0);
";

#[test]
fn given_loads_that_commit_in_batches_when_a_batch_fails_then_earlier_batches_stay_and_the_next_run_resumes()
-> Result<(), String> {
    run_async(|| async move {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let test_cases = [
            BatchedScenarioTestCase {
                description: "batch windows commit one by one; a failing window keeps the ones before it",
                setup: WINDOWED,
                settings: WINDOWED_SETTINGS,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("2026-11-06"),
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events_raw SET d = 1 WHERE id = 3;",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("2026-11-06"),
                        reload: false,
                        check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) || ' ' || CAST(count(DISTINCT _sqlload_load_id) AS VARCHAR) FROM raw.app__events",
                        show: "",
                    },
                    BatchedRun {
                        source_change: "DELETE FROM public.events_raw WHERE id = 4; INSERT INTO public.events_raw VALUES (5, '2026-11-04', 1);",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("2026-11-06"),
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                ],
                expected_outcomes: &[
                    "raw.app__events: Postgres query failed => 1,2",
                    "ok => 1,2,3,4 2",
                    "ok => 1,2,3,5",
                ],
            },
            BatchedScenarioTestCase {
                description: "windows read 4 at once still commit in order: a failing window keeps only the ones before it, and the next run resumes there",
                setup: WINDOWED,
                settings: WINDOWED_CONCURRENT,
                project: MICROBATCH_CONCURRENCY,
                batch_bytes: None,
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("2026-11-06"),
                        reload: false,
                        check_query: IDS,
                        show: "commits ",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events_raw SET d = 1 WHERE id = 3;",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("2026-11-06"),
                        reload: false,
                        check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) || ' ' || CAST(count(DISTINCT _sqlload_load_id) AS VARCHAR) FROM raw.app__events",
                        show: "note    reads over",
                    },
                ],
                expected_outcomes: &[
                    "raw.app__events: Postgres query failed => 1,2 | commits 5 windows of 1day, one commit each, read 4 at once",
                    "ok => 1,2,3,4 2 | note    reads over 4 connections, in one snapshot",
                ],
            },
            BatchedScenarioTestCase {
                description: "an ordered read commits between distinct cursor values; after a failure the next run reads on from the data",
                setup: ORDERED,
                settings: ORDERED_SETTINGS,
                project: "",
                batch_bytes: Some(1),
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("3"),
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "",
                        destination_change: "CREATE UNIQUE INDEX events_v ON raw.app__events (v)",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events SET v = 'e' WHERE id = 5;",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                ],
                expected_outcomes: &[
                    "ok => 1,2,3",
                    "raw.app__events: DuckDB => 1,2,3,4",
                    "ok => 1,2,3,4,5,6",
                ],
            },
            BatchedScenarioTestCase {
                description: "an unordered cursor read is one commit: a failure loads nothing, the next run loads every row once",
                setup: UNORDERED,
                settings: ORDERED_SETTINGS,
                project: "",
                batch_bytes: Some(1),
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.app__events",
                        show: "",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events_raw SET d = 1 WHERE id = 3;",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                ],
                expected_outcomes: &[
                    "raw.app__events: Postgres query failed => 0",
                    "ok => 1,2,3,4",
                ],
            },
            BatchedScenarioTestCase {
                description: "an integer batch without --end-cursor ends at the source's maximum, read at plan",
                setup: INTEGER,
                settings: INTEGER_SETTINGS,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "commits ",
                    },
                    BatchedRun {
                        source_change: "INSERT INTO public.events VALUES (6, 'f'), (7, 'g');",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "commits ",
                    },
                ],
                expected_outcomes: &[
                    "ok => 1,2,3,4,5 | commits 3 windows of 2, one commit each, up to the source's maximum 5 (read at plan)",
                    "ok => 1,2,3,4,5,6,7 | commits 2 windows of 2, one commit each, up to the source's maximum 7 (read at plan)",
                ],
            },
            BatchedScenarioTestCase {
                description: "--reload of an integer batch on a source that reports its maximum keeps only the range given, and without one ends at the maximum",
                setup: INTEGER,
                settings: INTEGER_SETTINGS,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "INSERT INTO public.events VALUES (6, 'f');",
                        destination_change: "",
                        start_cursor: Some("2"),
                        end_cursor: Some("4"),
                        reload: true,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: true,
                        check_query: IDS,
                        show: "commits ",
                    },
                ],
                expected_outcomes: &[
                    "ok => 1,2,3,4,5",
                    "ok => 2,3",
                    "ok => 1,2,3,4,5,6 | commits 3 windows of 2, read one after another into a new table, swapped in once at the end, up to the source's maximum 6 (read at plan)",
                ],
            },
            BatchedScenarioTestCase {
                description: "a cursor_start given as a date reaches timestamp batch windows as cursor text",
                setup: TIMESTAMPS,
                settings: START_AS_DATE,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: Some("2026-01-03"),
                    reload: false,
                    check_query: IDS,
                    show: "commits ",
                }],
                expected_outcomes: &["ok => 1,2 | commits 2 windows of 1day, one commit each"],
            },
            BatchedScenarioTestCase {
                description: "so does one given as RFC 3339; the plan notes the snapshot held open",
                setup: TIMESTAMPS,
                settings: START_AS_RFC3339,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: Some("2026-11-03"),
                    reload: false,
                    check_query: IDS,
                    show: "note    windows",
                }],
                expected_outcomes: &[
                    "ok => 3,4 | note    windows are read in one REPEATABLE READ snapshot, held open on the source for the whole table load (mind idle_in_transaction_session_timeout and vacuum)",
                ],
            },
            BatchedScenarioTestCase {
                description: "batch_size = none opts out of an ordered read: a failure keeps nothing of that read",
                setup: ORDERED,
                settings: WHOLE_SETTINGS,
                project: "",
                batch_bytes: Some(1),
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: Some("3"),
                        reload: false,
                        check_query: IDS,
                        show: "commits ",
                    },
                    BatchedRun {
                        source_change: "",
                        destination_change: "CREATE UNIQUE INDEX events_v ON raw.app__events (v)",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events SET v = 'e' WHERE id = 5;",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                ],
                expected_outcomes: &[
                    "ok => 1,2,3 | commits once, at the end of the read (batch_size = \"none\")",
                    "raw.app__events: DuckDB => 1,2,3",
                    "ok => 1,2,3,4,5,6",
                ],
            },
            BatchedScenarioTestCase {
                description: "delete_insert on an indexed cursor commits along the way: a row deleted upstream goes, and a later failure keeps the ranges before it",
                setup: DAYS,
                settings: DAYS_SETTINGS,
                project: "",
                batch_bytes: Some(1),
                concurrency: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "commits ",
                    },
                    BatchedRun {
                        source_change: "DELETE FROM public.events WHERE id = 4;",
                        destination_change: "",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                    BatchedRun {
                        source_change: "INSERT INTO public.events VALUES (6, '2026-11-06', 'f'), (7, '2026-11-07', NULL);",
                        destination_change: "ALTER TABLE raw.app__events ALTER COLUMN v SET NOT NULL",
                        start_cursor: None,
                        end_cursor: None,
                        reload: false,
                        check_query: IDS,
                        show: "",
                    },
                ],
                expected_outcomes: &[
                    "ok => 1,2,3,4,5 | commits about every 1 GB, in at order (an index leads with it, NULLs first), between distinct values; each commit replaces its own cursor range",
                    "ok => 1,2,3,5",
                    "raw.app__events: DuckDB => 1,2,3,5,6",
                ],
            },
            BatchedScenarioTestCase {
                description: "an ordered read commits NULL cursors first",
                setup: NULL_CURSORS,
                settings: ORDERED_SETTINGS,
                project: "",
                batch_bytes: Some(1),
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: None,
                    reload: false,
                    check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY rowid) FROM raw.app__events",
                    show: "",
                }],
                expected_outcomes: &["ok => 3,1,2"],
            },
            BatchedScenarioTestCase {
                description: "a text cursor is never read in order, even with an index",
                setup: TEXT_CURSOR,
                settings: ORDERED_SETTINGS,
                project: "",
                batch_bytes: Some(1),
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: None,
                    reload: false,
                    check_query: IDS,
                    show: "commits ",
                }],
                expected_outcomes: &[
                    "ok => 1,2 | commits once, at the end of the read (a text cursor, which the source and destination may sort differently)",
                ],
            },
            BatchedScenarioTestCase {
                description: "plan warns that replace ignores batch",
                setup: NO_CURSOR,
                settings: IGNORED_ON_REPLACE,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: None,
                    reload: false,
                    check_query: IDS,
                    show: "batch_size is ignored",
                }],
                expected_outcomes: &[
                    "ok => 1 | batch_size is ignored: replace swaps the whole table in once",
                ],
            },
            BatchedScenarioTestCase {
                description: "plan warns that a table committing no windows ignores batch_concurrency",
                setup: NO_CURSOR,
                settings: "write_strategy = \"replace\"\nbatch_concurrency = 4\n",
                project: MICROBATCH_CONCURRENCY,
                batch_bytes: None,
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: None,
                    reload: false,
                    check_query: IDS,
                    show: "batch_concurrency is ignored",
                }],
                expected_outcomes: &[
                    "ok => 1 | batch_concurrency is ignored: the table commits no batch_size windows",
                ],
            },
            BatchedScenarioTestCase {
                description: "plan warns that a table without a cursor ignores batch",
                setup: NO_CURSOR,
                settings: IGNORED_WITHOUT_CURSOR,
                project: "",
                batch_bytes: None,
                concurrency: None,
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: None,
                    reload: false,
                    check_query: IDS,
                    show: "batch_size is ignored",
                }],
                expected_outcomes: &[
                    "ok => 1 | batch_size is ignored: windows need a cursor_column",
                ],
            },
            BatchedScenarioTestCase {
                description: "tables load at once; a failing table leaves the others loaded",
                setup: PARALLEL,
                settings: "write_strategy = \"replace\"\nexclude = [\"public.*_raw\"]",
                project: "",
                batch_bytes: None,
                concurrency: Some(2),
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    start_cursor: None,
                    end_cursor: None,
                    reload: false,
                    check_query: "SELECT (SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__a) || ' ' || (SELECT string_agg(CAST(id AS VARCHAR), ',') FROM raw.app__b)",
                    show: "",
                }],
                expected_outcomes: &["raw.app__c: Postgres query failed => 1,2 3"],
            },
        ];
        for test_case in test_cases {
            assert_eq!(
                run_batched(&client, postgres.port, &test_case).await?,
                test_case.expected_outcomes,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_windows_read_at_once_when_the_first_is_slowest_or_the_source_moves_then_they_commit_in_order_from_one_snapshot()
-> Result<(), String> {
    let test_cases = [
        GatedWindowsTestCase {
            description: "the first window finishes reading last, and still commits first",
            settings: GATED_4_AT_ONCE,
            mid_load_change: "",
            check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY rowid) FROM raw.app__events",
            expected_outcome: "ok => 1,2,3,4",
        },
        GatedWindowsTestCase {
            description: "a row moved to a window read later is read once, as the snapshot every connection shares has it",
            settings: GATED_2_AT_ONCE,
            mid_load_change: "UPDATE public.events_raw SET at = '2026-11-04' WHERE id = 1; INSERT INTO public.events_raw VALUES (5, '2026-11-04');",
            check_query: "SELECT string_agg(CAST(id AS VARCHAR) || '@' || CAST(\"at\" AS VARCHAR), ',' ORDER BY id, \"at\") FROM raw.app__events",
            expected_outcome: "ok => 1@2026-11-01,2@2026-11-02,3@2026-11-03,4@2026-11-04",
        },
    ];
    run_async(|| async move {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let locker = connect(&postgres).await?;
        for test_case in test_cases {
            assert_eq!(
                run_gated((&client, &locker), postgres.port, &test_case).await?,
                test_case.expected_outcome,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
