use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{run_async, run_batched};
use crate::test_types::{BatchedRun, BatchedScenarioTestCase};

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
batch = "1d"
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
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let test_cases = [
            BatchedScenarioTestCase {
                description: "batch windows commit one by one; a failing window keeps the ones before it",
                setup: WINDOWED,
                settings: WINDOWED_SETTINGS,
                batch_bytes: None,
                threads: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        expected_error: "Postgres query failed",
                        end_cursor: Some("2026-11-06"),
                        check_query: IDS,
                        expected_result: "1,2",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events_raw SET d = 1 WHERE id = 3;",
                        destination_change: "",
                        expected_error: "",
                        end_cursor: Some("2026-11-06"),
                        check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) || ' ' || CAST(count(DISTINCT _sqlload_load_id) AS VARCHAR) FROM raw.app__events",
                        expected_result: "1,2,3,4 2",
                    },
                    BatchedRun {
                        source_change: "DELETE FROM public.events_raw WHERE id = 4; INSERT INTO public.events_raw VALUES (5, '2026-11-04', 1);",
                        destination_change: "",
                        expected_error: "",
                        end_cursor: Some("2026-11-06"),
                        check_query: IDS,
                        expected_result: "1,2,3,5",
                    },
                ],
            },
            BatchedScenarioTestCase {
                description: "an ordered read commits between distinct cursor values; after a failure the next run reads on from the data",
                setup: ORDERED,
                settings: ORDERED_SETTINGS,
                batch_bytes: Some(1),
                threads: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        expected_error: "",
                        end_cursor: Some("3"),
                        check_query: IDS,
                        expected_result: "1,2,3",
                    },
                    BatchedRun {
                        source_change: "",
                        destination_change: "CREATE UNIQUE INDEX events_v ON raw.app__events (v)",
                        expected_error: "app__events",
                        end_cursor: None,
                        check_query: IDS,
                        expected_result: "1,2,3,4",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events SET v = 'e' WHERE id = 5;",
                        destination_change: "",
                        expected_error: "",
                        end_cursor: None,
                        check_query: IDS,
                        expected_result: "1,2,3,4,5,6",
                    },
                ],
            },
            BatchedScenarioTestCase {
                description: "an unordered cursor read is one commit: a failure loads nothing, the next run loads every row once",
                setup: UNORDERED,
                settings: ORDERED_SETTINGS,
                batch_bytes: Some(1),
                threads: None,
                runs: &[
                    BatchedRun {
                        source_change: "",
                        destination_change: "",
                        expected_error: "Postgres query failed",
                        end_cursor: None,
                        check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.app__events",
                        expected_result: "0",
                    },
                    BatchedRun {
                        source_change: "UPDATE public.events_raw SET d = 1 WHERE id = 3;",
                        destination_change: "",
                        expected_error: "",
                        end_cursor: None,
                        check_query: IDS,
                        expected_result: "1,2,3,4",
                    },
                ],
            },
            BatchedScenarioTestCase {
                description: "tables load at once; a failing table leaves the others loaded",
                setup: PARALLEL,
                settings: "exclude = [\"public.*_raw\"]",
                batch_bytes: None,
                threads: Some(2),
                runs: &[BatchedRun {
                    source_change: "",
                    destination_change: "",
                    expected_error: "Postgres query failed",
                    end_cursor: None,
                    check_query: "SELECT (SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__a) || ' ' || (SELECT string_agg(CAST(id AS VARCHAR), ',') FROM raw.app__b)",
                    expected_result: "1,2 3",
                }],
            },
        ];
        for test_case in test_cases {
            run_batched(&client, postgres.port, &test_case).await?;
        }
        Ok(())
    })
}
