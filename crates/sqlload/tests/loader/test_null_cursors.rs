use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::models::PlanOptions;

use crate::batched_helpers::run_batched;
use crate::helpers::{described_project, duckdb_project, run_async, run_once, words};
use crate::test_types::{BatchedRun, BatchedScenarioTestCase, NullCursorPlanTestCase};

/// `public.t`: row 2 has no `updated_at`, only a `created_at`.
const SETUP: &str = "
CREATE TABLE public.t (id int8 PRIMARY KEY, v text, updated_at timestamptz, created_at timestamptz);
INSERT INTO public.t VALUES
  (1, 'a', '2026-11-01 10:00:00+00', '2026-10-01 10:00:00+00'),
  (2, 'b', NULL, '2026-10-02 10:00:00+00'),
  (3, 'c', '2026-11-02 10:00:00+00', '2026-10-03 10:00:00+00');
";

const ROWS: &str =
    "SELECT string_agg(CAST(id AS VARCHAR) || ':' || v, ',' ORDER BY id) FROM raw.app__t";

/// The first run reads everything; then row 2 is edited with its cursor still NULL, row 4 arrives with a NULL cursor, and row 1 gets a later cursor; last, row 2 gets its first cursor.
const RUNS: &[BatchedRun] = &[
    BatchedRun {
        source_change: "SELECT 1",
        destination_change: "",
        start_cursor: None,
        end_cursor: None,
        reload: false,
        check_query: ROWS,
        show: "",
    },
    BatchedRun {
        source_change: "UPDATE public.t SET v = 'b2' WHERE id = 2;
            INSERT INTO public.t VALUES (4, 'd', NULL, '2026-11-05 10:00:00+00');
            UPDATE public.t SET v = 'a2', updated_at = '2026-11-03 10:00:00+00' WHERE id = 1;",
        destination_change: "",
        start_cursor: None,
        end_cursor: None,
        reload: false,
        check_query: ROWS,
        show: "",
    },
    BatchedRun {
        source_change: "UPDATE public.t SET v = 'b3', updated_at = '2026-11-06 10:00:00+00' WHERE id = 2;",
        destination_change: "",
        start_cursor: None,
        end_cursor: None,
        reload: false,
        check_query: ROWS,
        show: "",
    },
];

#[test]
fn given_source_rows_with_a_null_cursor_when_loading_incrementally_then_each_null_cursor_rule_holds()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let test_cases = [
            BatchedScenarioTestCase {
                description: "by default a run whose filter would skip NULL-cursor rows fails, naming their count, and loads nothing",
                setup: SETUP,
                settings: r#"
[sources.app.tables."public.t"]
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "updated_at"
"#,
                batch_bytes: None,
                concurrency: None,
                runs: RUNS,
                expected_outcomes: &[
                    "ok => 1:a,2:b,3:c",
                    "raw.app__t: 2 source rows have a NULL updated_at, which an incremental read never selects, so they would never load; set null_cursor = \"include\" to read them on every run, list a column that is always set to coalesce with it (such as cursor_column = [\"updated_at\", \"created_at\"]), or set null_cursor = \"ignore\" to skip them => 1:a,2:b,3:c",
                    "raw.app__t: 1 source row has a NULL updated_at, which an incremental read never selects, so it would never load; set null_cursor = \"include\" to read them on every run, list a column that is always set to coalesce with it (such as cursor_column = [\"updated_at\", \"created_at\"]), or set null_cursor = \"ignore\" to skip them => 1:a,2:b,3:c",
                ],
            },
            BatchedScenarioTestCase {
                description: "null_cursor = \"include\" reads the NULL-cursor rows on every run, so the edited row 2 and the new row 4 load",
                setup: SETUP,
                settings: r#"
[sources.app.tables."public.t"]
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "updated_at"
null_cursor = "include"
"#,
                batch_bytes: None,
                concurrency: None,
                runs: RUNS,
                expected_outcomes: &[
                    "ok => 1:a,2:b,3:c",
                    "ok => 1:a2,2:b2,3:c,4:d",
                    "ok => 1:a2,2:b3,3:c,4:d",
                ],
            },
            BatchedScenarioTestCase {
                description: "null_cursor = \"ignore\" keeps skipping them: row 4 never loads and row 2 waits for its first cursor",
                setup: SETUP,
                settings: r#"
[sources.app.tables."public.t"]
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "updated_at"
null_cursor = "ignore"
"#,
                batch_bytes: None,
                concurrency: None,
                runs: RUNS,
                expected_outcomes: &[
                    "ok => 1:a,2:b,3:c",
                    "ok => 1:a2,2:b,3:c",
                    "ok => 1:a2,2:b3,3:c",
                ],
            },
            BatchedScenarioTestCase {
                description: "a coalesced cursor reads row 4 by its created_at, and row 2 once its updated_at is set",
                setup: SETUP,
                settings: r#"
[sources.app.tables."public.t"]
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = ["updated_at", "created_at"]
"#,
                batch_bytes: None,
                concurrency: None,
                runs: RUNS,
                expected_outcomes: &[
                    "ok => 1:a,2:b,3:c",
                    "ok => 1:a2,2:b,3:c,4:d",
                    "ok => 1:a2,2:b3,3:c,4:d",
                ],
            },
        ];
        for test_case in &test_cases {
            let outcomes = run_batched(&client, postgres.port, test_case).await?;
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
fn given_a_null_cursor_rule_when_planning_then_the_reads_line_names_it_and_include_is_refused_where_it_would_duplicate_rows()
-> Result<(), String> {
    let test_cases = [
        NullCursorPlanTestCase {
            description: "by default the reads line says a NULL cursor fails the load, as the source reports updated_at nullable",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"updated_at\"",
            expected_plan: "reads updated_at: first run, everything (NULL cursors: fail the load)",
        },
        NullCursorPlanTestCase {
            description: "null_cursor = \"include\" on merge reads them every run, and the reads line says so",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"updated_at\"\nnull_cursor = \"include\"",
            expected_plan: "reads updated_at: first run, everything (NULL cursors: included)",
        },
        NullCursorPlanTestCase {
            description: "null_cursor = \"ignore\" says they are skipped",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"updated_at\"\nnull_cursor = \"ignore\"",
            expected_plan: "reads updated_at: first run, everything (NULL cursors: skipped)",
        },
        NullCursorPlanTestCase {
            description: "a coalesced cursor names both columns",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = [\"updated_at\", \"created_at\"]",
            expected_plan: "reads coalesce(updated_at, created_at): first run, everything (NULL cursors: fail the load)",
        },
        NullCursorPlanTestCase {
            description: "null_cursor = \"include\" on append is refused, as every run would append the NULL rows again",
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"\nnull_cursor = \"include\"",
            expected_plan: "null_cursor = \"include\" reads the rows whose updated_at is NULL on every run, and append would append them again on every run, duplicating them; use write_strategy = \"merge\" with a unique_key",
        },
        NullCursorPlanTestCase {
            description: "null_cursor = \"include\" on delete_insert is refused, as no range it replaces holds the NULL rows",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"\nnull_cursor = \"include\"",
            expected_plan: "null_cursor = \"include\" reads the rows whose updated_at is NULL on every run, and they sit outside every cursor range delete_insert replaces, so it would insert them again on every run, duplicating them; use write_strategy = \"merge\" with a unique_key, or set null_cursor = \"ignore\" to skip them",
        },
        NullCursorPlanTestCase {
            description: "coalescing a timestamp with text is refused",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = [\"updated_at\", \"v\"]",
            expected_plan: "cursor_column coalesces updated_at (timestamp_tz) and v (text), which are different kinds of value",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let text = duckdb_project(
                postgres.port,
                &format!(
                    "include = [\"public.t\"]\n\n[sources.app.tables.\"public.t\"]\n{}\n",
                    test_case.settings
                ),
            );
            let destinations = connect_targets(&described_project(&text).await?)
                .await
                .map_err(|error| error.to_string())?;
            let plan = run_once(&text, &destinations, &PlanOptions::default())
                .await
                .unwrap_or_else(|error| error);
            assert!(
                words(&plan).contains(&words(test_case.expected_plan)),
                "{}: {plan}",
                test_case.description
            );
        }
        Ok(())
    })
}
