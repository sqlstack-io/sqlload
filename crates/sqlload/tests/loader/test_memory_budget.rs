//! Rows of 1 MiB loaded into a DuckDB file under a memory budget that gives DuckDB a `memory_limit` of 256MB, which one commit of 300 of them overflows.

use crate::helpers::run_async;
use crate::memory_helpers::{
    KILLED, LOAD, NEW_BODY, NEW_BODY_KILLED, NEW_BODY_LATER, NEW_BODY_TRIPPED, OLDER_MARKER, PLAN,
    expected_loads, missing_phrases, run_wide_load, wide_postgres,
};
use crate::test_types::WideLoadTestCase;

#[test]
fn given_300_rows_of_1_mib_when_loading_them_with_a_256mb_duckdb_limit_then_every_strategy_loads_them_in_pieces()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "replace builds the table in pieces, the first load and the next, under the budget the plan's memory line shows",
            table: "wide_replace",
            settings: "write_strategy = \"replace\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY, PLAN],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[
                "Memory  1.1 GiB budget (--memory-limit); ",
                "; DuckDB 488 MiB (memory_limit 244 MiB, 2 threads); ",
                " at once\n\nLoads (2)",
            ],
        },
        WideLoadTestCase {
            description: "append by an ordered cursor commits pieces straight into the table it creates",
            table: "wide_append",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(true, "300:300:0|2:2:0|0")],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "append without a cursor commits pieces straight into the table it creates",
            table: "wide_append_all",
            settings: "write_strategy = \"append\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(true, "300:300:0|2:2:0|0")],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "merge creates the table, then updates every row of it in pieces",
            table: "wide_merge",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"at\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY_LATER],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "merge by newest_by updates every row in pieces, each newer than the table's",
            table: "wide_newest",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"at\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY_LATER],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "merge sending its rows at commit holds them and sends them a piece at a time",
            table: "wide_held",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nsend = \"at_commit\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "delete_insert by replace_key replaces keys of ten rows in pieces cut between keys",
            table: "wide_keyed",
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
            rows: 300,
            key_rows: 10,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "delete_insert by cursor windows replaces each window in pieces cut between cursor values",
            table: "wide_window",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"at\"\nbatch_size = \"1 day\"\ncursor_start = \"2026-10-31T00:00:00Z\"\nlookback = \"100 days\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_rows_no_commit_can_hold_when_loading_them_then_only_their_table_stops_naming_the_memory_limit_it_needs()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "768 rows of 1 MiB fill one row group past what a checkpoint at 256MB rewrites: the table stops before it, the other loads",
            table: "wide_768",
            settings: "write_strategy = \"replace\"",
            rows: 768,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(false, "0:0:0|2:2:0|none")],
            expected_output: &["{table}", "row group", "memory_limit"],
        },
        WideLoadTestCase {
            description: "one replace_key value of 300 MiB can't be cut between keys: the table stops, keeping its rows",
            table: "wide_one_key",
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
            rows: 300,
            key_rows: 1000,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (false, "300:300:0|2:2:0|none"),
            ],
            expected_output: &["{table}", "one k value", "memory_limit"],
        },
        WideLoadTestCase {
            description: "past 90% of the budget the watchdog interrupts the largest table, which rolls back and fails, keeping its rows; the other loads",
            table: "wide_tripped",
            settings: "write_strategy = \"replace\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY_TRIPPED],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (false, "300:300:0|2:2:0|none"),
            ],
            expected_output: &["{table}", "90%", "memory_limit", "batch_size"],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_load_killed_between_its_pieces_when_it_runs_again_then_the_table_is_what_one_clean_load_leaves()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "append without a cursor: the killed load's first piece of 61 rows stays marked, and the rerun removes it before loading all 300",
            table: "killed_append_all",
            settings: "write_strategy = \"append\"",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[KILLED, LOAD],
            expected_results: &[(false, "61:61:0|1"), (true, "300:300:0|0")],
            expected_output: &["killed_append_all: removed the rows of load"],
        },
        WideLoadTestCase {
            description: "append by an unindexed cursor: the rerun removes the killed load's rows and reads from before it",
            table: "killed_unordered",
            settings: "write_strategy = \"append\"\ncursor_column = \"at2\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[KILLED, LOAD],
            expected_results: &[(false, "61:61:0|1"), (true, "300:300:0|0")],
            expected_output: &["killed_unordered: removed the rows of load"],
        },
        WideLoadTestCase {
            description: "append by a text cursor: the rerun removes the killed load's rows and reads from before it",
            table: "killed_text",
            settings: "write_strategy = \"append\"\ncursor_column = \"tag\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[KILLED, LOAD],
            expected_results: &[(false, "61:61:0|1"), (true, "300:300:0|0")],
            expected_output: &["killed_text: removed the rows of load"],
        },
        WideLoadTestCase {
            description: "delete_insert by replace_key: the killed load replaced the 19 rows of its first keys, and the rerun replaces every key",
            table: "killed_keyed",
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
            rows: 300,
            key_rows: 10,
            beside: &[],
            steps: &[LOAD, NEW_BODY_KILLED, LOAD],
            expected_results: &[
                (true, "300:300:0|none"),
                (false, "300:300:19|none"),
                (true, "300:300:300|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "delete_insert by cursor windows: the killed load replaced the 11 rows of its first window, whose day starts at 02:00 after the lookback, and 20 of the next, and the rerun replaces every window",
            table: "killed_window",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"at\"\nbatch_size = \"1 day\"\ncursor_start = \"2026-10-31T00:00:00Z\"\nlookback = \"100 days\"",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[LOAD, NEW_BODY_KILLED, LOAD],
            expected_results: &[
                (true, "300:300:0|none"),
                (false, "300:300:31|none"),
                (true, "300:300:300|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "a marker of another load removes only that load's three rows, never the rows of the load before it",
            table: "killed_older",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[LOAD, OLDER_MARKER],
            expected_results: &[(true, "300:300:0|0"), (true, "300:300:0|0")],
            expected_output: &["killed_older: removed the rows of load older-load"],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}
